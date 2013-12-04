require 'hiredis'
fs = require("fs")
redisx = require 'redis'
async = require 'async'


file = process.argv[2]
host = process.argv[3]
port = process.argv[4]

redis = redisx.createClient port, host
redis.on 'error', (err) ->
	console.log err

lineReader = require 'line-reader'
lineReader.eachLine file, (line, last, done) ->

	processLine line, (err) ->
		console.log err if err

		done() # err

		redis.quit() if last

ops =
	zset: (key, pairs, done) ->
		async.forEachLimit pairs, 200, (pair, next) ->
			redis.zadd [key, pair[1], pair[0]], next
		, done

lines = 0
processLine = (line, done) ->
	lines++
	if lines % 50 is 0
		process.title = "refeeder #{lines}"

	o = JSON.parse line

	ops[o.type] o.key, o.value, (err) ->
		if err
			console.log o.key + " failed", err
			return done()

		return done() unless o.ttl
		ttl = parseInt(o.ttl)
		return done() unless ttl > 0
		console.log ttl
		redis.expire o.key, ttl, done


