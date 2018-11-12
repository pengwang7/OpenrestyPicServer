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

local function load_pms_addr()
	return true, nil
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

local function get_params_for_uri()
	local authcode, uuid, filename = string.match(ngx.var.uri, "/upload/(%w+)/(%w+)/(%w+.%w+)")
	if not authcode or not uuid or not filename then
		ngx.log(ngx.ERR, "invalid uri format:", ngx.var.uri)
		return false, nil, nil
	end

	return string.format("%s_%s", uuid, filename), authcode, uuid
end

local function get_filename_from_boundary(ures)
	if type(ures) ~= "table" then
		return false
	end

	for _, v in ipairs(ures) do
		if type(v) == "string" then
			ngx.log(ngx.INFO, "v=", v)
			local filename = string.match(v, "filename=\"(%w+.%w+)\"")
			if filename then
				ngx.log(ngx.ERR, "filename=", filename)
				return filename
			end
		end
	end

	return false
end

local function select_from_redis(filename)
	--映射的关系都存在redis中 去redis中查找是否有同名文件
	local redis_opts = {}
	redis_opts["redis_ip"] = fdfs_redis_ip
	redis_opts["redis_port"] = fdfs_redis_port
	redis_opts["timeout"] = 3
	local redis_handler = redis_iresty:new(redis_opts)
	if not redis_handler then
		return nil, "redis_iresty:new failed"
	end

	local fileid, err = redis_handler:get("<KEY>_" .. filename)

	return fileid, err
end

--可以压缩的命令尽量使用pipeline进行压缩
local function upload_fileid_to_redis(filename, fileid, delete)	
	local redis_opts = {}
	redis_opts["redis_ip"] = fdfs_redis_ip
	redis_opts["redis_port"] = fdfs_redis_port
	redis_opts["timeout"] = 3
	local redis_handler = redis_iresty:new(redis_opts)
	if not redis_handler then
		return false, "redis_iresty new met error"
	end

	if delete == true then
		--start redis pipeline 
		redis_handler:init_pipeline()

		ngx.log(ngx.ERR, "delete filename:", filename, " fileid:", fileid)
		
		redis_handler:del(string.format("<KEY>_%s", filename))
		redis_handler:del(string.format("<KEY>_%s", fileid))

		local ok, err = redis_handler:commit_pipeline()
		if not ok then
			return false, err
		end
	else
		redis_handler:init_pipeline()

		redis_handler:set(string.format("<KEY>_%s", filename), fileid)
		redis_handler:set(string.format("<KEY>_%s", fileid), filename)

		local ok, err = redis_handler:commit_pipeline()
		if not ok then
			return false, err
		end
	end

	return true, nil
end

local function check_exist_and_delete(filename)
	local id, err = select_from_redis(filename)
	if err then
		return false, err
	end

	if not id and not err then
		return true, nil
	end


	local storage = storage_iresty:new()
	local tracker = tracker_iresty:new()
	if not storage or not tracker then
		return false, "create storage or tracker failed"
	end

	--设置超时时间 根据实际压力进行设置
	storage:set_timeout(3000)
	tracker:set_timeout(3000)

	local fdfs_tracker_opts = {}
	fdfs_tracker_opts["host"] = fdfs_tracker_ip
	fdfs_tracker_opts["port"] = fdfs_tracker_port
	local ok, err = tracker:connect(fdfs_tracker_opts)
	if not ok then
		return false, err
	end

	local ret, err = tracker:query_storage_update1(id)
	if not ret then
		return false, "query tracker met error:" .. err
	end

	local ok, err = storage:connect(ret)
	if not ok then
		return false, "storage connect tracker met error:" .. err
	end

	local cachefile = string.format("%s/%s", ngx.var.image_root, filename)
	ngx.log(ngx.ERR, "cachefile:", cachefile)

	--删除本地缓存图片
	if file_exist(cachefile) then
		os.execute("sudo rm " .. cachefile)
	end

	local ok, err = storage:delete_file1(id)
	if not ok then
		ngx.log(ngx.ERR, "storage:delete_file1 met error:", err, " file1id:", id)
--		return false, "storage delete file met error:" .. err
	end

	local ok, err = upload_fileid_to_redis(filename, id, true)
	if not ok then
		return false, err
	end

	--连接池 
	tracker:set_keepalive(60000, 1000)
	storage:set_keepalive(60000, 1000)

	return true, nil
end

local function image_upload_to_fastdfs()
	--配置文件中不要开启lua_need_request_body on;
	--这样会导致创建upload对象的失败
	local chuck_size = 8192
	local down_handler, err = upload_iresty:new(chunk_size)
	if not down_handler then
		return false, err
	end

	local storage = storage_iresty:new()
	local tracker = tracker_iresty:new()
	if not storage or not tracker then
		return false, "create storage or tracker failed"
	end

	--设置超时时间 根据实际压力进行设置
	storage:set_timeout(3000)
	tracker:set_timeout(3000)

	local fileid = nil
	
	while true do
		local utype, ures, uerr = down_handler:read()
		if not utype then
			return false, uerr
		end

		local fdfs_tracker_opts = {}
		fdfs_tracker_opts["host"] = fdfs_tracker_ip
		fdfs_tracker_opts["port"] = fdfs_tracker_port
		
		if utype == "header" then
			local flags = get_filename_from_boundary(ures)
			if flags then
				--如果是只有一个storage 可以不用让tracker去查询
				--直接去连接 提高上传的效率
				local ok, err = tracker:connect(fdfs_tracker_opts)
				local ret, err = tracker:query_storage_store()
				if not ret then
					return false, err
				end

				local ok, err = storage:connect(ret)
				if not ok then
					return false, err
				end
			end

		elseif utype == "body" then
			if not fileid then
				--创建追加类型的文件 通过返回的结果可以知道文件存储的位置
				local ret, err = storage:upload_appender_by_buff(ures, "jpeg")
				if not ret or type(ret) ~= "table" then
					
				end
				--组成fileid
				fileid = string.format("%s/%s", ret.group_name, ret.file_name)
				ngx.log(ngx.INFO, "fileid=", fileid)
			else
				--拿到文件上传的位置 向文件中追加内容
				local ok, err = storage:append_by_buff1(fileid, ures)
				if not ok then
					return false, err
				end
			end

		elseif utype == "part_end" then
			if fileid then
				storage:set_keepalive(60000, 1000)
				tracker:set_keepalive(60000, 1000)

				return true, fileid
			end

		elseif utype == "eof" then
			break
		end
	end

end

local function process_start()
	local filename, authcode, uuid = get_params_for_uri()
	if not filename or not authcode or not uuid then
		ngx.log(ngx.ERR, "get params met for uri failed")
		return http_reply_message(ngx.HTTP_INTERNAL_SERVER_ERROR, "invalid uri format")
	end

	ngx.log(ngx.INFO, "filename:", filename, "|auth:", authcode, "|uuid:", uuid)

--	local ok, err = authcode_iresty.check_authcode(auth_redis_ip, auth_redis_port, uuid, authcode, "Read")
--	if not ok then
--		ngx.log(ngx.ERR, "authcode is error")
--	end

	local filelock = lock_iresty:new("my_locks")
	local ok, err = filelock:lock(filename)
	if not ok then
		return http_reply_message(ngx.HTTP_INTERNAL_SERVER_ERROR, "lock file error:" .. filename)
	end
 
	local ok, err = check_exist_and_delete(filename)
	if not ok then
		local lock_ok, lock_err = filelock:unlock(filename)
		if not lock_ok then
			ngx.log(ngx.ERR, "file unlock met error:", lock_err)
		end

		return http_reply_message(ngx.HTTP_INTERNAL_SERVER_ERROR, "check filename exist failed")
	end

	local ok, fileid = image_upload_to_fastdfs()
	if not ok then
		local lock_ok, lock_err = filelock:unlock(filename)
		if not lock_ok then
			ngx.log(ngx.ERR, "file unlock met error:", lock_err)
		end

		ngx.log(ngx.ERR, "image_upload_to_fastdfs met error:", err)
		return http_reply_message(ngx.HTTP_INTERNAL_SERVER_ERROR, "image upload fastdfs failed")
	end

	ngx.log(ngx.ERR, "-------------------------fileaname:", filename, "|fileid:", fileid)
	local ok, err = upload_fileid_to_redis(filename, fileid, false)
	if not ok then
		ngx.log(ngx.ERR, "upload_fileid_to_redis met error:", err)
		return http_reply_message(ngx.HTTP_INTERNAL_SERVER_ERROR, "update fileid to redis failed")
	end

	local ok, err = filelock:unlock(filename)
	if not ok then
		ngx.log(ngx.ERR, "filelock unlock met error:", err);
	end

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








































