-- Lua script to import kosen dataset
-- Automatically execute following steps
-- - Import original data
-- - Generate dataset
-- - Annotation

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
-- src_path/
--   |- cnn/
--   |   |- train/
--   |   |    |- image
--   |   |
--   |   |- val/
--   |   |    |- image
--   |   |
--   |   |- test/
--   |        |- image
--   |
--   |- yolo/
--       |- train/
--       |    |- image
--       |
--       |- val/
--       |    |- image
--       |
--       |- annotations/
--       |    |- train.json
--       |    |- val.json
local params = {
  common = {
    src_path = "/product/worker/assets/dani_dataset",
    purpose = "training",
    class_domain_name = "fieldworkers",
    classes = {
      "0001_A.testudinarium_Am_D",
      "0002_A.testudinarium_Am_A",
      "0003_A.testudinarium_Am_H",
      "0004_A.testudinarium_Af_D",
      "0005_A.testudinarium_Af_A",
      "0006_A.testudinarium_Af_H",
      "0007_A.testudinarium_N_D",
      "0008_A.testudinarium_N_A",
      "0009_A.testudinarium_N_H",
      "0010_A.testudinarium_N_Hd",
      "0011_H.flava_Am_D",
      "0012_H.flava_Am_A",
      "0013_H.flava_Am_H",
      "0014_H.flava_Af_D",
      "0015_H.flava_Af_A",
      "0016_H.flava_Af_H",
      "0017_H.flava_N_D",
      "0018_H.flava_N_A",
      "0019_H.flava_N_Hd",
      "0020_H.flava_N_Ha",
      "0021_H.formosensis_Am_D",
      "0022_H.formosensis_Am_A",
      "0023_H.formosensis_Am_H",
      "0024_H.formosensis_Af_D",
      "0025_H.formosensis_Af_A",
      "0026_H.formosensis_Af_H",
      "0027_H.formosensis_N_D",
      "0028_H.formosensis_N_A",
      "0029_H.formosensis_N_Hd",
      "0030_H.formosensis_N_Ha",
      "0031_H.hystricis_Am_D",
      "0032_H.hystricis_Am_A",
      "0033_H.hystricis_Am_H",
      "0034_H.hystricis_Af_D",
      "0035_H.hystricis_Af_A",
      "0036_H.hystricis_Af_H",
      "0037_H.hystricis_N_D",
      "0038_H.hystricis_N_A",
      "0039_H.hystricis_N_Hd",
      "0040_H.hystricis_N_Ha",
      "0041_H.kitaokai_Am_D",
      "0042_H.kitaokai_Am_A",
      "0043_H.kitaokai_Am_H",
      "0044_H.kitaokai_Af_D",
      "0045_H.kitaokai_Af_A",
      "0046_H.kitaokai_Af_H",
      "0047_H.kitaokai_N_D",
      "0048_H.kitaokai_N_A",
      "0049_H.kitaokai_N_Hd",
      "0050_H.kitaokai_N_Ha",
      "0051_H.longicornis_Am_D",
      "0052_H.longicornis_Am_A",
      "0053_H.longicornis_Am_H",
      "0054_H.longicornis_Af_D",
      "0055_H.longicornis_Af_A",
      "0056_H.longicornis_Af_H",
      "0057_H.longicornis_N_D",
      "0058_H.longicornis_N_A",
      "0059_H.longicornis_N_Hd",
      "0060_H.longicornis_N_Ha",
      "0061_H.megaspinosa_Am_D",
      "0062_H.megaspinosa_Am_A",
      "0063_H.megaspinosa_Am_H",
      "0064_H.megaspinosa_Af_D",
      "0065_H.megaspinosa_Af_A",
      "0066_H.megaspinosa_Af_H",
      "0067_H.megaspinosa_N_D",
      "0068_H.megaspinosa_N_A",
      "0069_H.megaspinosa_N_Hd",
      "0070_H.megaspinosa_N_Ha",
      "0071_I.nipponiensis_Am_D",
      "0072_I.nipponiensis_Am_A",
      "0073_I.nipponiensis_Am_H",
      "0074_I.nipponiensis_Af_D",
      "0075_I.nipponiensis_Af_A",
      "0076_I.nipponiensis_Af_H",
      "0077_I.nipponiensis_N_D",
      "0078_I.nipponiensis_N_A",
      "0079_I.nipponiensis_N_Hd",
      "0080_I.nipponiensis_N_Ha",
      "0081_I.ovatus_Am_D",
      "0082_I.ovatus_Am_A",
      "0083_I.ovatus_Am_H",
      "0084_I.ovatus_Af_D",
      "0085_I.ovatus_Af_A",
      "0086_I.ovatus_Af_H",
      "0087_I.ovatus_N_D",
      "0088_I.ovatus_N_A",
      "0089_I.ovatus_N_Hd",
      "0090_I.ovatus_N_Ha",
      "0091_I.persulcatus_Am_D",
      "0092_I.persulcatus_Am_A",
      "0093_I.persulcatus_Am_H",
      "0094_I.persulcatus_Af_D",
      "0095_I.persulcatus_Af_A",
      "0096_I.persulcatus_Af_H",
      "0097_I.persulcatus_N_D",
      "0098_I.persulcatus_N_A",
      "0099_I.persulcatus_N_Hd",
      "0100_I.persulcatus_N_Ha",
      "0101_I.turdus_Am_D",
      "0102_I.turdus_Am_A",
      "0103_I.turdus_Am_H",
      "0104_I.turdus_Af_D",
      "0105_I.turdus_Af_A",
      "0106_I.turdus_Af_H",
      "0107_I.turdus_N_D",
      "0108_I.turdus_N_A",
      "0109_I.turdus_N_Hd",
      "0110_I.turdus_N_Ha",
    }
  },
  resource = {"yolo", "cnn"}
}
params.yolo = {
  original = {
    dirname = "yolo",
    src_path = {
      train = u.join_path(params.common.src_path, "yolo/train"),
      val = u.join_path(params.common.src_path, "yolo/val")
    }
  },
  dataset = {
    task_type = "OBJECT_DETECTION",
  },
  annotation = {
    file_path = {
      train = u.join_path(params.common.src_path, "yolo/annotations/train.json"),
      val = u.join_path(params.common.src_path, "yolo/annotations/val.json")
    }
  }
}
params.cnn = {
  original = {
    dirname = "cnn_dani",
  },
  dataset = {
    task_type = "IMAGE_CLASSIFICATION",
  },
  annotation = {
    dir_path = {
      train = {
        u.join_path(params.common.src_path, "cnn/train/0001_A.testudinarium_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0002_A.testudinarium_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0003_A.testudinarium_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0004_A.testudinarium_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0005_A.testudinarium_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0006_A.testudinarium_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0007_A.testudinarium_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0008_A.testudinarium_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0009_A.testudinarium_N_H"),
        u.join_path(params.common.src_path, "cnn/train/0010_A.testudinarium_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0011_H.flava_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0012_H.flava_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0013_H.flava_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0014_H.flava_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0015_H.flava_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0016_H.flava_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0017_H.flava_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0018_H.flava_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0019_H.flava_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0020_H.flava_N_Ha"),
        u.join_path(params.common.src_path, "cnn/train/0021_H.formosensis_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0022_H.formosensis_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0023_H.formosensis_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0024_H.formosensis_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0025_H.formosensis_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0026_H.formosensis_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0027_H.formosensis_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0028_H.formosensis_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0029_H.formosensis_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0030_H.formosensis_N_Ha"),
        u.join_path(params.common.src_path, "cnn/train/0031_H.hystricis_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0032_H.hystricis_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0033_H.hystricis_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0034_H.hystricis_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0035_H.hystricis_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0036_H.hystricis_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0037_H.hystricis_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0038_H.hystricis_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0039_H.hystricis_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0040_H.hystricis_N_Ha"),
        u.join_path(params.common.src_path, "cnn/train/0041_H.kitaokai_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0042_H.kitaokai_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0043_H.kitaokai_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0044_H.kitaokai_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0045_H.kitaokai_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0046_H.kitaokai_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0047_H.kitaokai_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0048_H.kitaokai_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0049_H.kitaokai_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0050_H.kitaokai_N_Ha"),
        u.join_path(params.common.src_path, "cnn/train/0051_H.longicornis_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0052_H.longicornis_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0053_H.longicornis_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0054_H.longicornis_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0055_H.longicornis_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0056_H.longicornis_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0057_H.longicornis_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0058_H.longicornis_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0059_H.longicornis_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0060_H.longicornis_N_Ha"),
        u.join_path(params.common.src_path, "cnn/train/0061_H.megaspinosa_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0062_H.megaspinosa_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0063_H.megaspinosa_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0064_H.megaspinosa_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0065_H.megaspinosa_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0066_H.megaspinosa_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0067_H.megaspinosa_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0068_H.megaspinosa_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0069_H.megaspinosa_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0070_H.megaspinosa_N_Ha"),
        u.join_path(params.common.src_path, "cnn/train/0071_I.nipponiensis_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0072_I.nipponiensis_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0073_I.nipponiensis_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0074_I.nipponiensis_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0075_I.nipponiensis_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0076_I.nipponiensis_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0077_I.nipponiensis_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0078_I.nipponiensis_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0079_I.nipponiensis_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0080_I.nipponiensis_N_Ha"),
        u.join_path(params.common.src_path, "cnn/train/0081_I.ovatus_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0082_I.ovatus_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0083_I.ovatus_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0084_I.ovatus_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0085_I.ovatus_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0086_I.ovatus_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0087_I.ovatus_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0088_I.ovatus_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0089_I.ovatus_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0090_I.ovatus_N_Ha"),
        u.join_path(params.common.src_path, "cnn/train/0091_I.persulcatus_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0092_I.persulcatus_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0093_I.persulcatus_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0094_I.persulcatus_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0095_I.persulcatus_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0096_I.persulcatus_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0097_I.persulcatus_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0098_I.persulcatus_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0099_I.persulcatus_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0100_I.persulcatus_N_Ha"),
        u.join_path(params.common.src_path, "cnn/train/0101_I.turdus_Am_D"),
        u.join_path(params.common.src_path, "cnn/train/0102_I.turdus_Am_A"),
        u.join_path(params.common.src_path, "cnn/train/0103_I.turdus_Am_H"),
        u.join_path(params.common.src_path, "cnn/train/0104_I.turdus_Af_D"),
        u.join_path(params.common.src_path, "cnn/train/0105_I.turdus_Af_A"),
        u.join_path(params.common.src_path, "cnn/train/0106_I.turdus_Af_H"),
        u.join_path(params.common.src_path, "cnn/train/0107_I.turdus_N_D"),
        u.join_path(params.common.src_path, "cnn/train/0108_I.turdus_N_A"),
        u.join_path(params.common.src_path, "cnn/train/0109_I.turdus_N_Hd"),
        u.join_path(params.common.src_path, "cnn/train/0110_I.turdus_N_Ha"),
      },
      val = {},
      test = {}
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

local function create_original_data(
  name, 
  purpose, 
  opts
)
  -- create original data directory
  local file_tags = kDbHandler:select({
    file_tags = {
      cols = {"id"},
      where = {
        name = purpose,
        is_deleted = false
      }
    }
  })
  if #file_tags == 0 then return end
  cti.logger.debug("file_tags:", file_tags, ", #file_tags:", #file_tags)
  local file_tag_id = file_tags and file_tags[1].id
  
  local original_data_directory_path, err = original_data.create_directory(name, {tag_id = file_tag_id})
  cti.logger.debug("original_data_directory_path:", original_data_directory_path)
  
  -- import original data
  -- cnnのアノテーションデータ生成を容易にするため{path = ファイルパス, class_name = クラス名}のようなオブジェクトを作る
  local src_file_paths = {}
  if opts.task_type == "OBJECT_DETECTION" then
    src_file_paths = u.list_append(src_file_paths, u.map(cc.fs.list_files(opts.src_train_path), function(f)
      return {path = u.join_path(opts.src_train_path, f)}
    end))
    src_file_paths = u.list_append(src_file_paths, u.map(cc.fs.list_files(opts.src_val_path), function(f)
      return {path = u.join_path(opts.src_val_path, f)}
    end))
  elseif opts.task_type == "IMAGE_CLASSIFICATION" then
    for k, v in ipairs(params.cnn.annotation.dir_path.train) do
      local res, err = cc.fs.list_files(v)
      if res then
        src_file_paths = u.list_append(src_file_paths, u.map(res, function(f)
          return {path = u.join_path(v, f), class_name = params.common.classes[k] .. "@" .. params.common.class_domain_name}
        end))
      end
    end
  else
    cti.logger.err("Invalid task_type: ", opts.task_type)
    return
  end
  for _, src_file_path in pairs(src_file_paths) do
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
  end
  return original_data_directory_path
end

local function create_dataset(
  name,
  task_type,
  opts
)
  -- create dataset directory
  local dataset_directory_path, err = dataset.create_directory(name, task_type)
  cti.logger.debug("dataset_directory_path:", dataset_directory_path)

  -- create dataset data
  local file_tags = kDbHandler:select({
    file_tags = {
      cols = {"id"},
      where = {
        name = purpose,
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
    opts.original_data_directory_path .. "/%"
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

local function create_class(
  class_domain_name,
  classes
)
  -- create class domain, classes and associte them
  kDbHandler:insert({
    core_class_domains = {
      values = {
        name = class_domain_name,
        description = class_domain_name
      }
    }
  })

  local class_domain_id = kDbHandler:query(string.format([[
    SELECT `id` FROM `core_class_domains`
    WHERE `name` = '%s'
  ]], class_domain_name))
  class_domain_id = class_domain_id[1].id

  for _, c in pairs(classes) do
    local class_name = c .. "@" .. class_domain_name
    local class_name_display = c
    kDbHandler:insert({
      core_classes = {
        values = {
          name = class_name,
          name_display = class_name_display,
          description = class_name_display
        }
      }
    })

    local class_id = kDbHandler:query(string.format([[
      SELECT `id` FROM `core_classes`
      WHERE `name` = '%s'
    ]], class_name))
    class_id = class_id[1].id

    kDbHandler:insert({
      map_core_classes_core_class_domains = {
        values = {
          class_domain_id = class_domain_id,
          class_id = class_id 
        }
      }
    })
  end
  return class_domain_id
end

local function associate_class_to_dataset(
  class_domain_name,
  dataset_directory_path
)
  -- associate specific class domain's classes to dataset
  local class_domain_id = kDbHandler:query(string.format([[
    SELECT `id` FROM `core_class_domains`
    WHERE `name` = '%s'
  ]], class_domain_name))
  class_domain_id = class_domain_id[1].id

  local mccccds = kDbHandler:query(string.format([[
    SELECT * FROM `map_core_classes_core_class_domains`
    WHERE `class_domain_id` = '%s'
  ]], class_domain_id))
  
  kDbHandler:query(string.format([[
      INSERT INTO `core_class_dataset_map` %s
    ]],
    kDb.make_insert_values_stmt(u.map(mccccds, function(mccccd)
      return {
        dataset_path = dataset_directory_path,
        class_id_default = mccccd.class_id,
        class_id = mccccd.class_id,
        class_domain_id = mccccd.class_domain_id
      }
    end))
  ))
end

local function create_annotation_yolo(
  annotation_file_paths,
  dataset_directory_path
)
  -- annotation
  -- annotation column format in annotations table
  -- for OBJECT_DETECTION
  -- {
  --   class_id: 1234,
  --   segmentation: [
  --     [100, 200, 300, 200, 250, 120, 200, 100]
  --   ],
  --   bbox: [100, 100, 600, 500],
  --   width: 1280,
  --   height: 920,
  -- }
  -- annotations table need file_path too
  --
  -- how to get above information from annotation file
  -- file_path: u.join_path(dataset_directory_path, images[i].file_name)
  -- width, height: images[i].width, images[i].height
  -- bbox: annotations(which has images[i].file_name).bbox, can be multiple per an image
  -- segmentation: annotations(which has images[i].file_name).segmentation, can be multiple per an image
  -- class_id: annotations(which has images[i].file_name).category_id, can be multiple per an image
  -- convert class_id: 1 into values for "flat" in core_classes
  --                   2 into values for "dense" in core_classes
  --                   3 into values for "sparse" in core_classes
  --                   4 into values for "dense2" in core_classes
  for _, ann_path in pairs(annotation_file_paths) do
    local ann = read_file(ann_path)
    ann = u.json.decode(ann)
    local ann_images = ann.images
    local ann_classes = ann.categories
    for _, img in pairs(ann_images) do
      local anns_for_img = u.filter(ann.annotations, function(a) return img.id == a.image_id end)
      for _, ann_for_img in pairs(anns_for_img) do
        local ann_data = {}
        ann_data.class_id = ann_for_img.category_id
        ann_data.segmentation = ann_for_img.segmentation
        ann_data.bbox = ann_for_img.bbox
        ann_data.width = img.width
        ann_data.height = img.height
        local class_name = u.find(ann_classes, function(a) return ann_data.class_id == a.id end).name
        local class_id = kDbHandler:select({
          core_classes = {
            cols = {"id"},
            where = {name_display = class_name}
          }
        })
        class_id = class_id[1].id
        ann_data.class_id = tonumber(class_id)
        local img_path = u.join_path(dataset_directory_path, img.file_name)
        kDbHandler:insert({
          annotations = {
            values = {
              file_path = img_path,
              annotation = u.json.encode(ann_data)
            }
          }
        })
      end
    end
  end
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

local function yolo()
  local original_data_directory_path = create_original_data(
    params.yolo.original.dirname,
    params.common.purpose,
    {
      src_train_path = params.yolo.original.src_path.train,
      src_val_path = params.yolo.original.src_path.val,
      task_type = params.yolo.dataset.task_type
    }
  )
  local dataset_directory_path = create_dataset(
    params.yolo.original.dirname,
    params.yolo.dataset.task_type,
    {
      original_data_directory_path = original_data_directory_path
    }
  )
  associate_class_to_dataset(
    params.common.class_domain_name,
    dataset_directory_path
  )
  create_annotation_yolo(
    params.yolo.annotation.file_path,
    dataset_directory_path
  )
  return true
end

local function cnn()
  local original_data_directory_path = create_original_data(
    params.cnn.original.dirname,
    params.common.purpose,
    {
      task_type = params.cnn.dataset.task_type
    }
  )
  local dataset_directory_path = create_dataset(
    params.cnn.original.dirname,
    params.cnn.dataset.task_type,
    {
      original_data_directory_path = original_data_directory_path
    }
  )
  associate_class_to_dataset(
    params.common.class_domain_name,
    dataset_directory_path
  )
  create_annotation_cnn(
    original_data_directory_path,
    dataset_directory_path
  )
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
    and create_class(params.common.class_domain_name, params.common.classes)
    -- and yolo()
    and cnn()
end

return {3, proc}
