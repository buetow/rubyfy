#!/usr/bin/env ruby

# (C) 2015 by Paul Buetow 

require "getoptlong"
require "json"
require "net/http"
require "net/ssh"
require "pp"
require "thread"

$ARGS = Hash.new
$LOG_MUTEX = Mutex.new

def log(severity, message)
  return if severity == :VERBOSE and not $ARGS["--verbose"]
  $LOG_MUTEX.synchronize do
    puts "#{severity}::#{message}"
  end
end

def run_command(server, command="uptime", root=false, user=ENV["USER"])
  log(:VERBOSE,"#{server}::Connecting")
  sudo = root ? "sudo " : ""
  Net::SSH.start(server, user) do |session|
    log(:VERBOSE, "#{server}::Executing #{sudo}#{command}")
    session.exec!("#{sudo}#{command}") do |channel, stream, data|
      log(:OUT, "#{server}::#{data}") unless $ARGS["--silent"]
    end
  end
end

def run_job(job)
  server = job[:SERVER]
  command = job[:COMMAND]
  root = job[:ROOT]
  log(:VERBOSE, "#{server}::Running job #{job}")
  if File.exists?("#{server}.ignore")
    log(:INFO, "#{server}::Ignoring this server")
  else
    run_command server, command, root
  end
  job[:STATUS] = :OK
end

def http_get(uri_str, content_type="application/json")
  uri = URI.parse(uri_str)
  req = Net::HTTP::Get.new(uri.path)
  req.[]=("Accept", content_type)
  http = Net::HTTP.new(uri.host, uri.port)
  http.request(req).body
end

begin
  opts = GetoptLong.new(
    [ "--command", "-c", GetoptLong::REQUIRED_ARGUMENT ],
    [ "--parallel", "-p", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--silent", "-s", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--verbose", "-v", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--debug", "-d", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--root", "-r", GetoptLong::OPTIONAL_ARGUMENT ],
  )

  opts.each do |opt, arg|
    $ARGS[opt] = arg
  end

  if $ARGS["--debug"]
    opts.each do |opt, arg|
      puts "#{opt} #{arg}"
    end
  end

  servers, jobs = [], []
  STDIN.read.split("\n").each { |s| servers << s }

  work_q = Queue.new
  servers.each do |server|
    job = {
      :SERVER => server,
      :COMMAND => $ARGS["--command"],
      :ROOT => $ARGS["--root"],
      :STATUS => :NONE,
    }
    jobs << job
    work_q.push(job)
  end

  parallel = $ARGS["--parallel"].to_i

  threads = (1..parallel).map do
    Thread.new do
      begin
        while job = work_q.pop(true)
          run_job(job)
        end
      rescue ThreadError => e
      rescue => e
        log(:ERROR, "#{job[:SERVER]}::#{e.message}")
        log(:ERROR, "#{job[:SERVER]}::#{e.inspect}")
      end
    end
  end

  threads.map(&:join)
  log(:INFO, "-::Done processing all servers")

  jobs.each do |job|
    if job[:STATUS] != :OK
      log(:WARN,"#{job[:SERVER]}::No job result")
    end
  end
end
