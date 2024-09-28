-- Lua script to divide images for DLTool

local cc = require("cc")
local u = require("util")
local cti = require("cti")
local cv = require("opencv")


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
  }
}
params.cnn = {
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

local function divide(source_path, target_dir_path, opts)
  local function padding_img(img, opts)
    opts = opts or {}
    local width, height = img.width, img.height
    local new_width = math.ceil(width / opts.sx) * opts.sx
    local new_height = math.ceil(height / opts.sy) * opts.sy
    local borderType = cv.BORDER_CONSTANT
    local top = 0
    local bottom = new_height - height
    local left = 0
    local right = new_width - width
    local padded = cv.copyMakeBorder(img, top, bottom, left, right, borderType, cv.kwargs({ value = { 114, 114, 114 } }))
    return padded
  end

  local function divide2d(img, cell_size)
    cell_size = cell_size or {}
    local width, height = img.width, img.height
    local sx, sy = cell_size.sx, cell_size.sy
    if not (sx and sy) then
      return nil, "Error divide size not provided"
    end
    if width % sx ~= 0 or height % sy ~= 0 then
      img = padding_img(img, {
        sx = sx,
        sy = sy
      })
    end
  
    local cells = {}
    for j = 1, height, sy do
      for i = 1, width, sx do
        cells[#cells + 1] = img:new({ i - 1, j - 1, sx, sy })
      end
    end
    return cells
  end

  local img, err = cv.imread(source_path)
  if not img or err then
    return nil, err
  end
  local basename = u.basename(source_path)
  local ext = u.extension(basename)

  local cells, err = divide2d(img, { sx = opts.sx, sy = opts.sy })
  if not cells or err then
    return nil, err
  end
  
  for i = 1, #cells do
    local path = u.join_path(target_dir_path, string.sub(basename, 1, #basename - #ext) .. "_" .. i .. ext)
    local res, err = cv.imwrite(path, cells[i])
    if not res or err then
      return nil, err
    end
  end
end


local function divide_images()
  for k, v in ipairs(params.cnn.annotation.dir_path) do
    local res, err
    local tmp_dir_path = u.dirname(v) .. u.basename(v) .. ".tmp"
    res, err = cc.fs.create_directory(tmp_dir_path)
    res, err = cc.fs.list_files(v)
    local filepaths = u.map(res, function(r)
      return u.join_path(v, r)
    end)
    for _, filepath in ipairs(filepaths) do
      exec_fn_except_dotfiles(u.basename(filepath), function()
        divide(
          filepath,
          tmp_dir_path,
          {
            sx = 180,
            sy = 180
          }
        )
      end)
    end
    res, err = cc.fs.delete_path(v)
    if not res or err then
      cti.logger.err(err)
      return
    end
    res, err = cc.fs.move(tmp_dir_path, v)
    if not res or err then
      cti.logger.err(err)
      return
    end
  end
  return true
end

local function proc()
  return divide_images()
end

return {3, proc}
