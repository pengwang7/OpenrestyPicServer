local cjson_iresty = require("cjson.safe")
local lock_iresty = require("resty.lock")
local upload_iresty = require("resty.upload")
local tracker_iresty = require("resty.fastdfs.tracker")
local storage_iresty = require("resty.fastdfs.storage")
local redis_iresty = require("common_lua.redis_iresty")

local fdfs_tracker_ip = "127.0.0.1"
local fdfs_tracker_port = 22122
local fdfs_redis_ip = "127.0.0.1"
local fdfs_redis_port = 6379

function http_reply_message(status_code, message)
	ngx.log(ngx.ERR, "status_code:", status_code, "type:", type(status_code))
	ngx.status = status_code
	if not message or not type(message) ~= "string" then
		ngx.exit(ngx.status)
	end

	ngx.header.content_length = string.len(message)
	ngx.say(message)
end

local function file_exist(file)
	local fd = io.open(file, "r")
	if not fd then
		return false
	else
		io.close(fd)
		return true
	end
end

local function is_dir(path)
	if type(path) ~= "string" then
		return false
	end
	
	local ret = os.execute("cd " .. path)
	if ret == 0 then
		return true
	else
		return false
	end
end

local function writefile(original_image, data)
	local fd = io.open(original_image .. ".jpeg", "w")
	if not fd then
		return false
	end
	
	fd:write(data)
	fd:close(fd)
	
	return true
end

local function read_fileid_from_redis(real_image_name)
	local opts = {}
	opts["redis_ip"] = fdfs_redis_ip
	opts["redis_port"] = fdfs_redis_port
	opts["timeout"] = 3
	
	local redis_handler = redis_iresty:new(opts)
	if not redis_handler then
		return false, "redis_iresty:new met error:redis_handler is nil"
	end
	
	ngx.log(ngx.ERR, "redis key:", "<KEY>_" .. real_image_name)
	
	local fileid = redis_handler:get("<KEY>_" .. real_image_name .. ".jpeg")
	if not fileid then
		return false, "redis_handler:get failed"
	end
	
	return fileid, nil
end

local function download_from_fdfs(real_image_name, original_image)
	local fileid, err = read_fileid_from_redis(real_image_name)
	if not fileid then
		return false, err 
	end
	
	local opts = {}
	opts["host"] = fdfs_tracker_ip
	opts["port"] = fdfs_tracker_port
	
	local tracker = tracker_iresty:new()
	if not tracker then
		return false, "tracker_iresty:new failed"
	end
	
	tracker:set_timeout(3000)
	local ok, err = tracker:connect(opts)
	if not ok then
		return false, err
	end
	
	local ret, err = tracker:query_storage_fetch1(fileid)
	if not ret then
		return false, err
	end
	
	local storage = storage_iresty:new()
	storage:set_timeout(3000)
	
	local ok, err = storage:connect(ret)
	if not ok then
		return false, err
	end
	
	local data, err = storage:download_file_to_buff1(fileid)
	if not data then
		return false, err
	end
	
	if not is_dir(ngx.var.image_root) then
		os.execute("mkdir -p " .. ngx.var.image_root)
	end
	
	ngx.log(ngx.ERR, "original_image:", original_image)
	local ok = writefile(original_image, data)
	if not ok then
		return false, "local writefile failed"
	end
	
	tracker:set_keepalive(60000, 1000)
	storage:set_keepalive(60000, 1000)
	
	return true, nil
end

local function load_pms_addr()
	return true, nil
end

local function process_start()
	local uuid = ngx.var.image_uuid
	local auth = ngx.var.image_authcode
	local name = ngx.var.image_name
	local all_name = ngx.var.image_all_name
	
	ngx.log(ngx.ERR, "uuid:", uuid, " auth:", auth, " name:", name, " all_name:", all_name)
	
	local area = ""
	local real_image_name = name
	local index = string.find(name, "_([0-9]+)x([0-9]+)")
	if index then
		ngx.log(ngx.ERR, "index:", index)
		real_image_name = string.sub(name, 0, index - 1)
		area = string.sub(name, index + 1, -1)
		index = string.find(area, "([.])")
		if not index then
			return false
		end
		area = string.sub(area, 0, index - 1)
	end
	
	ngx.log(ngx.ERR, "real_image_name:", real_image_name)
	
	local original_image = string.format("%s/%s", ngx.var.image_root, real_image_name)
	if not file_exist(original_image) then
		local ok, err = download_from_fdfs(real_image_name, original_image)
		if not ok then
			ngx.log(ngx.ERR, "download_from_fdfs met error:", err)
			
			
		end
	end

	if area then
		local cmd = string.format("gm convert %s -thumbnail %s -background gray -gravity center -extent %s %s", original_image .. ".jpeg", area, area, all_name)
		ngx.log(ngx.ERR, "cmd=", cmd)
		os.execute(cmd)
	end
--[[	
	if file_exist(all_name) then
		local internal_uri = string.format("/download_internal/%s", ngx.var.image_name)
		ngx.exec(internal_uri)
	else
		ngx.exit(ngx.HTTP_NOT_FOUND)
	end
	]]--
	ngx.log(ngx.ERR, "real_image_name:", real_image_name, " area:", area)
--[[	
    if index then
        uuidfilename = string.sub(ngx.var.image_name, 0, index-1);
        area = string.sub(ngx.var.image_name, index+1);
        index = string.find(area, "([.])");
        area = string.sub(area, 0, index-1);
    end

    print("uuidfilename===",uuidfilename)
    print("area===",area)
    local originalFile = ngx.var.image_root.."/"..uuidfilename
    print("originalFile===",originalFile)

    --ÅÐ¶ÏÒ»ÏÂÔ­Ê¼ÎÄ¼þÊÇ·ñ´æÔÚ£¬Èç¹û²»´æÔÚÔòÒª´ÓfdfsÖÐÏÂÔØÏÂÀ´
    if not file_exists(originalFile) then
        local ok,err = download_uuidfile_from_fdfs(uuidfilename,originalFile)
        if not ok then
            ngx.log(ngx.ERR,"download_uuidfile_from_fdfs failed")
            return send_resp_data(ngx.HTTP_INTERNAL_SERVER_ERROR,"download_uuidfile_from_fdfs failed")
        end
    end
]]--
	
	
	return true
end

if (ngx.var.server_port == "8081") then
	service_type = "pms picserver"
	local ok, err = load_pms_addr()
	if not ok then
		ngx.log(ngx.ERR, "load_pms_addr met error:", err)
	end
else
	ngx.log(ngx.ERR, "invalid pms picserver port")
end

process_start()