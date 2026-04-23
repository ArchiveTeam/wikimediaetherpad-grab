local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")
local html_entities = require("htmlEntities")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local seen_200 = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local retry_url = false
local is_initial_url = true

local item_patterns = {
  ["^https?://etherpad%.wikimedia%.org/p/([^/]+)/export/etherpad$"] = "pad",
}

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
--print("discovered", item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  if ids[url] then
    return nil
  end
  local value = nil
  local type_ = nil
  for pattern, name in pairs(item_patterns) do
    value = string.match(url, pattern)
    type_ = name
    if value then
      break
    end
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    local newcontext = {}
    new_item_type = found["type"]
    new_item_value = found["value"]
    new_item_name = new_item_type .. ":" .. new_item_value
    if new_item_name ~= item_name then
      ids = {}
      context = newcontext
      item_value = new_item_value
      item_type = new_item_type
      ids[string.lower(item_value)] = true
      ids[string.lower("padId=" .. item_value)] = true
      context["token"] = nil
      context["sid"] = nil
      context["timeslider_rev"] = nil
      context["timeslider_request"] = nil
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = new_item_name
      print("Archiving item " .. item_name)
    end
  end
end

percent_encode_url = function(url)
  temp = ""
  for c in string.gmatch(url, "(.)") do
    local b = string.byte(c)
    if b < 32 or b > 126 then
      c = string.format("%%%02X", b)
    end
    temp = temp .. c
  end
  return temp
end

allowed = function(url, parenturl)
  local noscheme = string.match(url, "^https?://(.*)$")

  if ids[url]
    or (noscheme and ids[string.lower(noscheme)]) then
    return true
  end

  local skip = false
  local discovered = false
  for pattern, type_ in pairs(item_patterns) do
    match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, percent_encode_url(new_item))
        skip = true
        discovered = true
      end
    end
  end
  if skip then
    return false
  end

  for _, pattern in pairs({
    "([^/%?&;]+)"
  }) do
    for s in string.gmatch(url, pattern) do
      if ids[string.lower(s)] then
        return true
      end
    end
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  return false
end

decode_codepoint = function(newurl)
  newurl = string.gsub(
    newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
    function (s)
      return utf8.char(tonumber(s, 16))
    end
  )
  return newurl
end

percent_encode_url = function(newurl)
  result = string.gsub(
    newurl, "(.)",
    function (s)
      local b = string.byte(s)
      if b < 32 or b > 126 then
        return string.format("%%%02X", b)
      end
      return s
    end
  )
  return result
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil

  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl, headers, post_data)
    if not string.match(newurl, "^https?://") then
      return nil
    end
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0
      or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      local url_data = {
        url=url_,
        headers=headers
      }
      if post_data then
        url_data["post_data"] = post_data
      end
      table.insert(urls, url_data)
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function set_new_params(newurl, data)
    for param, value in pairs(data) do
      if value == nil then
        value = ""
      elseif type(value) == "string" then
        value = "=" .. value
      end
      if string.match(newurl, "[%?&]" .. param .. "=") then
        newurl = string.gsub(newurl, "([%?&]" .. param .. "=)[^%?&;]*", "%1" .. string.gsub(value, "^=", ""))
      else
        if string.match(newurl, "%?") then
          newurl = newurl .. "&"
        else
          newurl = newurl .. "?"
        end
        newurl = newurl .. param .. value
      end
    end
    return newurl
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return set_new_params(newurl, {[param]=tostring(value)})
    else
      if default ~= nil then
        default = tostring(default)
      end
      return set_new_params(newurl, {[param]=default})
    end
  end

  local function get_count(data)
    local count = 0
    for _ in pairs(data) do
      count = count + 1
    end 
    return count
  end

  local function check_payload(newurl, payload)
    print("Sending", payload)
    local payload_length = utf8.len(payload)
    check(
      newurl,
      {
        ["Cookie"] = "token=" .. context["token"],
        ["Content-Type"] = "text/plain;charset=UTF-8",
        ["Origin"] = "https://etherpad.wikimedia.org"
      },
      payload_length .. ":" .. payload
    )
  end

  if allowed(url)
    and status_code < 300 then
    html = read_file(file)
    if item_type == "pad" then
      if not context["token"] then
        context["token"] = "t."
        local chars = "0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz"
        for _ = 1, 20 do
          local n = math.random(string.len(chars))
          context["token"] = context["token"] .. string.sub(chars, n, n)
        end
      end
      local socket_prefix = "https://etherpad.wikimedia.org/socket.io/?padId=" .. item_value .. "&EIO=3&transport=polling"
      local payload_item_value = urlparse.unescape(item_value)
      local headers = {
        ["Cookie"] = "token=" .. context["token"]
      }
      check("https://etherpad.wikimedia.org/p/" .. item_value)
      check("https://etherpad.wikimedia.org/p/" .. item_value .. "/timeslider")
      check("https://etherpad.wikimedia.org/p/" .. item_value .. "/export/etherpad")
      check("https://etherpad.wikimedia.org/p/" .. item_value .. "/export/html")
      check("https://etherpad.wikimedia.org/p/" .. item_value .. "/export/txt")
      check(socket_prefix .. "&t=0", headers)
      local new_sid = string.match(html, '"sid":"([^"]+)"')
      if new_sid
        and (
          url == socket_prefix .. "&t=0"
          or url == socket_prefix .. "&t=10"
        ) then
        context["sid"] = new_sid
        check(
          set_new_params(increment_param(url, "t", nil, 1), {sid=new_sid}),
          headers
        )
      end
      local t = tonumber(string.match(url, "[%?&]t=([0-9]+)"))
      local sid = string.match(url, "[%?&]sid=([^&]+)") or context["sid"]
      if sid then
        context["sid"] = sid
        local payload = nil
        if string.match(html, "2:40") then
          if t == 1 then
            payload = "42[\"message\",{\"component\":\"pad\",\"type\":\"CLIENT_READY\",\"padId\":\"" .. payload_item_value .. "\",\"token\":\"" .. context["token"] .. "\",\"userInfo\":{\"colorId\":null,\"name\":null}}]"
          elseif t == 11 then
            payload = "42[\"message\",{\"component\":\"pad\",\"type\":\"CLIENT_READY\",\"data\":{},\"padId\":\"" .. payload_item_value .. "\",\"token\":\"" .. context["token"] .. "\"}]"
          end
        end
        if payload then
          check_payload(increment_param(url, "t", nil, 1), payload)
        end
        if t == 2
          or t == 12 then
          check(increment_param(url, "t", nil, 1), headers)
        end
        local queue_req = false
        if t == 13
          and string.match(html, '"type":"CLIENT_VARS"') then
          context["timeslider_rev"] = tonumber(string.match(
            html,
            '"collab_client_vars":{.-"rev":([0-9]+)'
          ))
          context["timeslider_request"] = 0
          queue_req = true
        elseif context["timeslider_request"] ~= nil
          and string.match(html, '"type":"CHANGESET_REQ"') then
          context["timeslider_request"] = context["timeslider_request"] + 1
          queue_req = true
        elseif context["timeslider_request"] ~= nil
          and not string.match(html, "^ok$") then
          check(increment_param(url, "t", nil, 1), headers)
        end
        if context["timeslider_request"] ~= nil
          and string.match(html, "^ok$") then
          check(increment_param(url, "t", nil, 1), headers)
        end
        if queue_req then
          local start = nil
          local granularity = nil
          local n = context["timeslider_request"]
          for _, info in ipairs({
            {["min"]=10001, ["step"]=10000, ["granularity"]=100},
            {["min"]=1001, ["step"]=1000, ["granularity"]=10},
            {["min"]=1, ["step"]=100, ["granularity"]=1}
          }) do
            if context["timeslider_rev"]
              and context["timeslider_rev"] >= info["min"]
              and start == nil then
              local count = math.floor((context["timeslider_rev"] - 1) / info["step"]) + 1
              if n < count then
                start = n * info["step"]
                granularity = info["granularity"]
              end
              n = n - count
            end
          end
          if start ~= nil
            and granularity ~= nil then
            local id = math.random(100000) - 1
            payload = "42[\"message\",{\"component\":\"pad\",\"type\":\"CHANGESET_REQ\",\"data\":{\"start\":" .. start .. ",\"granularity\":" .. granularity .. ",\"requestID\":" .. id .. "},\"padId\":\"" .. payload_item_value .. "\",\"token\":\"" .. context["token"] .. "\"}]"
            check_payload(increment_param(url, "t", nil, 1), payload)
          end
        end
      end
      if url == "https://etherpad.wikimedia.org/p/" .. item_value .. "/timeslider" then
        check(socket_prefix .. "&t=10", headers)
      end
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if http_stat["statcode"] ~= 200 then
    retry_url = true
    return false
  end
  if http_stat["len"] == 0
    and http_stat["statcode"] < 300 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]

  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    if string.match(url["url"], "^https://etherpad%.wikimedia%.org/socket%.io/") then
      abort_item()
      return wget.actions.ABORT
    end
    tries = tries + 1
    local maxtries = 5
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    if status_code == 200 then
      if not seen_200[url["url"]] then
        seen_200[url["url"]] = 0
      end
      seen_200[url["url"]] = seen_200[url["url"]] + 1
    end
    downloaded[url["url"]] = true
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end
