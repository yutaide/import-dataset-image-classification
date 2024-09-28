-- Lua script to import image classification dataset to DLTool
-- Automatically execute importing images and annotation

local cc = require("cc")
local u = require("util")
local env = require("env")
local cti = require("cti")
local file = require("file")
local original_data = require("original-data")
local dataset = require("dataset")
local uuid = require("uuid")
uuid.seed()

local prefix = env.cti_data_dir()

-- make sure file structure is proper
-- main_directory/
--   |- class1/
--   |    |- image1
--   |    |- image2
--   |    |- ...
--   |    |- imageN
--   ...
--   |- classN/
--   |    |- image1
--   |    |- image2
--   |    |- ...
--   |    |- imageN


local params = {
  common = {
    src_path = "/product/worker/assets/main_directory",
    purpose = "training",
    class_domain_name = "nagase_i"
  }
}
params.cnn = {
  original = {
    dirname = "whetstone",
  },
  dataset = {
    task_type = "IMAGE_CLASSIFICATION",
  },
  annotation = {
    dir_path = {
      u.join_path(params.common.src_path, "afterdress"),
      u.join_path(params.common.src_path, "ap03"),
      u.join_path(params.common.src_path, "ap06"),
      u.join_path(params.common.src_path, "ap09"),
      u.join_path(params.common.src_path, "ap18")
    }
  }
}

local function abs_path(path)
  return u.join_path(prefix, path)
end

local function read_file(path)
  local file = io.open(path, "rb")
  if not file then
    return false
  end
  local file_content = file:read("*all")
  file:close()
  return file_content
end

local function exec_fn_except_dotfiles(v, fn)
  -- Skip dotfile such as .DS_Store
  if not v:match("^%.") then
    fn()
  end
end

local function create_original_data()
  -- create original data directory
  local file_tags = kDbHandler:select({
    file_tags = {
      cols = {"id"},
      where = {
        name = params.common.purpose,
        is_deleted = false
      }
    }
  })
  if #file_tags == 0 then return end
  cti.logger.debug("file_tags:", file_tags, ", #file_tags:", #file_tags)
  local file_tag_id = file_tags and file_tags[1].id
  
  local original_data_directory_path, err = original_data.create_directory(params.cnn.original.dirname, {tag_id = file_tag_id})
  cti.logger.debug("original_data_directory_path:", original_data_directory_path)
  
  -- import original data
  -- cnnのアノテーションデータ生成を容易にするため{path = ファイルパス, class_name = クラス名}のようなオブジェクトを作る
  local src_file_paths = {}
  for k, v in ipairs(params.cnn.annotation.dir_path) do
    local res, err = cc.fs.list_files(v)
    local class_name_display = u.basename(v)
    if res then
      src_file_paths = u.list_append(src_file_paths, u.map(res, function(f)
        return {path = u.join_path(v, f), class_name = class_name_display .. "@" .. params.common.class_domain_name}
      end))
    end
  end
  for _, src_file_path in pairs(src_file_paths) do
    exec_fn_except_dotfiles(u.basename(src_file_path.path), function()
      local dst_file_path = u.join_path(original_data_directory_path, u.basename(src_file_path.path))
      local res, err = cc.fs.copy(src_file_path.path, abs_path(dst_file_path))
      cti.logger.debug("res of copy: ", res or err)
      if not res or err then
        -- Rename file and retry
        local basename = u.basename(src_file_path.path)
        local ext = u.extension(src_file_path.path)
        dst_file_path = u.join_path(original_data_directory_path, basename:sub(1, #basename - #ext) .. "_" .. uuid() .. ext)
        res, err = cc.fs.copy(src_file_path.path, abs_path(dst_file_path))
        cti.logger.debug("[Retry]res of copy: ", res or err)
      end
      kDbHandler:insert({
        files = {
          values = {
            path = dst_file_path,
            description = src_file_path.class_name
          }
        }
      })
      kDbHandler:insert({
        map_files_file_tags = {
          values = {
            file_path = dst_file_path,
            file_tag_id = file_tag_id
          }
        }
      })
    end)
  end
  return original_data_directory_path
end

local function create_dataset(original_data_directory_path)
  -- create dataset directory
  local dataset_directory_path, err = dataset.create_directory(params.cnn.original.dirname, params.cnn.dataset.task_type)
  cti.logger.debug("dataset_directory_path:", dataset_directory_path)

  -- create dataset data
  local file_tags = kDbHandler:select({
    file_tags = {
      cols = {"id"},
      where = {
        name = params.common.purpose,
        is_deleted = false
      }
    }
  })
  if #file_tags == 0 then return end
  cti.logger.debug("file_tags:", file_tags, ", #file_tags:", #file_tags)
  local file_tag_id = file_tags and file_tags[1].id

  local original_datas = kDbHandler:query(string.format([[
      SELECT `path` FROM `files`
      WHERE `path` LIKE '%s'
    ]],
    original_data_directory_path .. "/%"
  ))
  cti.logger.debug("original_datas:", original_datas)

  for _, original_data in pairs(original_datas) do
    local dst_file_path = u.join_path(dataset_directory_path, u.basename(original_data.path))
    local res, err = cc.fs.copy(abs_path(original_data.path), abs_path(dst_file_path))
    cti.logger.debug("res of copy: ", res or err)
    kDbHandler:insert({
      files = {
        values = {
          path = dst_file_path
        }
      }
    })
    kDbHandler:insert({
      map_files_file_tags = {
        values = {
          file_path = dst_file_path,
          file_tag_id = file_tag_id
        }
      }
    })
  end
  return dataset_directory_path
end

local function create_class()
  -- create class domain, classes and associte them
  kDbHandler:insert({
    core_class_domains = {
      values = {
        name = params.common.class_domain_name,
        description = params.common.class_domain_name
      }
    }
  })

  local class_domain_id = kDbHandler:select({
    core_class_domains = {
      cols = {"id"},
      where = {name = params.common.class_domain_name}
    }
  }) 
  class_domain_id = class_domain_id[1].id

  for _, c in pairs(params.cnn.annotation.dir_path) do
    local class_name_display = u.basename(c)
    local class_name = class_name_display .. "@" .. params.common.class_domain_name

    kDbHandler:insert({
      core_classes = {
        values = {
          name = class_name,
          name_display = class_name_display,
          description = class_name_display
        }
      }
    })

    local class_id = kDbHandler:select({
      core_classes = {
        cols = {"id"},
        where = {name = class_name}
      }
    })
    class_id = class_id[1].id

    kDbHandler:insert({
      map_core_classes_core_class_domains = {
        values = {
          class_domain_id = class_domain_id,
          class_id = class_id,
          description = class_name
        }
      }
    })
  end
  return class_domain_id
end

local function associate_class_to_dataset(dataset_directory_path)
  -- associate specific class domain's classes to dataset
  local class_domain_id = kDbHandler:select({
    core_class_domains = {
      cols = {"id"},
      where = {name = params.common.class_domain_name}
    }
  })
  class_domain_id = class_domain_id[1].id

  local mccccds = kDbHandler:select({
    map_core_classes_core_class_domains = {
      where = {class_domain_id = class_domain_id}
    }
  })
  
  kDbHandler:insert({
    core_class_dataset_map = {
      values = u.map(mccccds, function(mccccd)
        return {
          dataset_path = dataset_directory_path,
          class_id_default = mccccd.class_id,
          class_id = mccccd.class_id,
          class_domain_id = mccccd.class_domain_id
        }
      end)
    }
  })
end

local function create_annotation_cnn(original_data_directory_path, dataset_directory_path)
  local original_files = kDbHandler:query(string.format([[
    SELECT * FROM `files`
    WHERE `path` LIKE '%s'
  ]], original_data_directory_path .. "/%"))

  local files = u.map(original_files, function(f)
    -- オリジナルファイルに関するレコードのpath部分だけdatasetファイルのpathに置換
    f.path = u.join_path(dataset_directory_path, u.basename(f.path))
    return f
  end)

  local classes = kDbHandler:select({
    core_classes = {
      cols = {
        "id",
        "name"
      }
    }
  })

  -- FIXME: オリジナルファイルは分割なし、学習用ファイルは分割ありの場合はファイル数が一致しないのでその場合は使用不可
  for _, f in pairs(files) do
    local ann_data
    for _, c in pairs(classes) do
      if c.name == f.description then
        ann_data = u.json.encode({{class_id = tonumber(c.id)}})
        break
      end
    end
    kDbHandler:insert({
      annotations = {
        values = {
          file_path = f.path,
          annotation = ann_data
        }
      }
    })
  end
end

local function cnn()
  local original_data_directory_path = create_original_data()
  local dataset_directory_path = create_dataset(original_data_directory_path)
  associate_class_to_dataset(dataset_directory_path)
  create_annotation_cnn(original_data_directory_path, dataset_directory_path)
  return true
end

local function check_init_db()
  for _, tag in pairs(original_data.tags) do
    local res = kDbHandler:select({
      file_tags = {
        where = {
          name = tag
        }
      }
    })
    if #res == 0 then return end
  end
  cti.logger.debug("init-db process has finished")
  return true
end

local function proc()
  return 
    check_init_db()
    and create_class()
    and cnn()
end

return {3, proc}
