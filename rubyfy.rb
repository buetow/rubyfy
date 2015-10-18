#!/usr/bin/env ruby

# (C) 2015 by Paul Buetow 

require "getoptlong"
require "json"
require "net/http"
require "net/ssh"
require "pp"
require "thread"

class Rubyfy
  def initialize(opts)
    @conf = Hash.new
    @log_mutex = Mutex.new

    opts.each do |opt, arg|
      opt.sub!(/^-+/, '')
      @conf[opt] = arg
    end

    @conf["verbose"] = true if @conf["debug"]

    # Read first config found
    ["#{ENV["HOME"]}/.rubyfy.json", "rubyfy.json"].each do |conf_path|
      if File.exists?(conf_path)
        log(:VERBOSE, "Reading #{conf_path}")
        conf_json = JSON.parse(File.read(conf_path))
        log(:VERBOSE, conf_json)
        conf_json.each do |opt, arg|
          @conf[opt] = arg unless @conf[arg]
        end
        break
      end
    end

    # Needed a 2nd time (as we read the config file)
    @conf["verbose"] = true if @conf["debug"]

    # Set defaults of values if not set
    @conf["parallel"] = 1 unless @conf["parallel"]
    @conf["user"] = ENV["USER"] unless @conf["user"]
    @conf["outdir"] = "./results" unless @conf["outdir"]

    log(:DEBUG, @conf)
  end

  def run
    servers, jobs = [], []
    STDIN.read.split("\n").each { |s| servers << s }

    work_q = Queue.new
    servers.each do |server|
      job = {
        :SERVER => server,
        :COMMAND => @conf["command"],
        :ROOT => @conf["root"],
        :USER => @conf["user"],
        :STATUS => :NONE,
      }
      jobs << job
      work_q.push(job)
    end

    parallel = @conf["parallel"].to_i

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

private

  def run_command(server, command="uptime", root=false, user=ENV["USER"])
    log(:VERBOSE,"#{server}::Connecting")
    sudo = root ? "sudo " : ""
    Net::SSH.start(server, user) do |session|
      log(:VERBOSE, "#{server}::Executing #{sudo}#{command}")
      session.exec!("#{sudo}#{command}") do |channel, stream, data|
        log(:OUT, "#{server}::#{data}") unless @conf["silent"]
      end
    end
  end

  def run_job(job)
    server = job[:SERVER]
    command = job[:COMMAND]
    root = job[:ROOT]
    user = job[:USER]
    log(:VERBOSE, "#{server}::Running job #{job}")
    if File.exists?("#{server}.ignore")
      log(:INFO, "#{server}::Ignoring this server")
    else
      run_command server, command, root, user
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

  def log(severity, message)
    return if severity == :VERBOSE and not @conf["verbose"]
    return if severity == :DEBUG and not @conf["debug"]
    @log_mutex.synchronize do
      puts "#{severity}::#{message}"
    end
  end
end

begin
  opts = GetoptLong.new(
    [ "--command", "-c", GetoptLong::REQUIRED_ARGUMENT ],
    [ "--debug", "-d", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--name", "-n", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--outdir", "-o", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--parallel", "-p", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--root", "-r", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--silent", "-s", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--user", "-u", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--verbose", "-v", GetoptLong::OPTIONAL_ARGUMENT ],
  )

  Rubyfy.new(opts).run
end
