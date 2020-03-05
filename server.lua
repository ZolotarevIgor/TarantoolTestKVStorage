#!/usr/bin/env tarantool

local json = require("json")
local log = require("log")

local conf = {}

--[[
	Reading config file from "key=value" format
]]
local loadconf, err = loadfile("tarantool.conf", "t", conf)
if loadconf then
	loadconf()
else
	log.error("Error reading config: " .. err)
	return
end

box.cfg{
	listen = conf.listen or 3301,
	log_format = conf.log_format or "plain",
	log = conf.log or "tarantool.log",
	background = conf.background or true,
	pid_file = conf.pid or "tarantool.pid"
}

local init_function = function()
	kv = box.schema.space.create("kv")
	kv:format({
		{name = "key", type = "string"},
		{name = "value", type = "string"},
	})

	box.space.kv:create_index("pk", {type = "hash", parts = {"key"}})
	box.schema.user.grant("guest", "read,write,execute", "universe")
end

box.once("kv", init_function)

--[[
	POST handler
	Input: request
	Returns JSON with error description in case of error, else returns status OK
]]
local post_handler = function(req)
	log.info("Request for inserting value")
	local status, body = pcall(req.json, req)
	if not status then
		return {
			status = 400,
			body = '{"error": "Invalid JSON"}'
		}
	end
	local key, val = body["key"], body["value"]

	if (status == false) or (type(key) ~= "string") or
			(type(val) ~= "table") then
		log.error("Error: invalid data")
		return { status = 400, body = '{"error": "Invalid data"}' }
	end

	local status, data = pcall(box.space.kv.insert, box.space.kv, {key, val})

	if status then
		return { status = 200 }
	else
		log.error("Error: " .. data)
		return {
			status = 409,
			body = '{"error": "'.. data ..'"}'
		}
	end
end

--[[
	PUT handler
	Input: request
	Returns JSON with error description in case of error, else returns status OK
]]
local put_handler = function(req)
	local key = req:stash("key")
	log.info("Request for updating by key " .. key)
	local status, body = pcall(req.json, req)
	if not status then
		return {
			status = 400,
			body = '{"error": "Invalid JSON"}'
		}
	end
	local val = body["value"]

	if (status == false) or (type(val) ~= "table") then
		log.error("Error: invalid data")
		return { status = 400, body = '{"error": "Invalid data"}' }
	end
	local status, data = pcall(box.space.kv.update, box.space.kv, key, { {"=", 2, val} })

	if data == nil then
		log.error("Error: key not found: " .. key )
		return { status = 404, body = '{"error": "Key not found"}' }
	elseif status then
		return { status = 200 }
	else
		log.error("Error: " .. data)
		return {
			status = 409,
			body = '{"error": "'.. data ..'"}'
		}
	end
end

--[[
	GET handler
	Input: request
	Returns JSON with error description in case of error, else returns JSON
]]
local get_handler = function(req)
	local key = req:stash("key")
	log.info("Request for getting " .. key)
	local status, data = pcall(
		box.space.kv.get, box.space.kv, key)

	if status and data then
		return {
			status = 200,
			body = json.encode(data[2])
		}
	elseif data == nil then
		log.error("Error: key not found: " .. key )
			return { status = 404, body = '{"error": "Key not found"}' }
	else
		log.error("Error: " .. data)
		return {
			status = 500,
			body = '{"error": "'.. data ..'"}'
		}
	end
end

--[[
	DELETE handler
	Input: request
	Returns JSON with error description in case of error, else returns JSON
]]
local delete_handler = function(req)
	local key = req:stash("key")
	log.info("Request for deletion " .. key)
	local status, data = pcall(box.space.kv.delete, box.space.kv, key)
	if status and data then
		return {
			status = 200,
			body = json.encode(data[2])
		}
	elseif data == nil then
		log.error("Error: key not found: " .. key )
			return { status = 404, body = '{"error": "Key not found"}' }
	else
		log.error("Error: " .. data)
		return {
			status = 500,
			body = '{"error": '.. data ..'}'
		}
	end
end


local server = require("http.server").new(conf.host, conf.port)
router = require('http.router').new({charset="application/json"})
server:set_router(router)

router:route({path = "/kv/:key", method = "GET"}, get_handler)

router:route({path = "/kv", method = "POST"}, post_handler)

router:route({path = "/kv/:key", method = "PUT"}, put_handler)

router:route({path = "/kv/:key", method = "DELETE"}, delete_handler)

server:start()