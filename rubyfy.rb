#!/usr/bin/env ruby

# (C) 2015 by Paul Buetow 

require "fileutils"
require "getoptlong"
require "json"
require "net/http"
require "net/scp"
require "net/ssh"
require "pp"
require "thread"

class Rubyfy
  def initialize(opts)
    @conf = Hash.new
    @log_mutex = Mutex.new
    @outfile = nil
    @outfile_mode = "w"

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
          @conf[opt] = arg unless @conf[opt]
        end
        break
      end
    end

    # Needed a 2nd time (as we read the config file)
    @conf["verbose"] = true if @conf["debug"]

    # Set defaults of values if not set
    @conf["parallel"] = 1 unless @conf["parallel"]
    @conf["user"] = ENV["USER"] unless @conf["user"]

    # Dealing where to write the output to
    @conf["outdir"] = "./out" unless @conf["outdir"]
    @conf["name"] = "#{ENV["USER"]}" unless @conf["name"]
    @outfile = "#{@conf["outdir"]}/#{@conf["name"]}"
    FileUtils.mkdir_p(@conf["outdir"]) unless File.directory?(@conf["outdir"])

    log(:DEBUG, @conf)
  end

  def run
    servers, jobs = [], []
    STDIN.read.split("\n").each do |line|
      line.split(" ").each { |s| servers << s }
    end

    log(:VERBOSE, "Server list: #{servers}")

    work_q = Queue.new
    servers.each do |server|
      job = {
        :BACKGROUND => @conf["background"],
        :COMMAND => @conf["command"],
        :PRECONDITION => @conf["precondition"],
        :ROOT => @conf["root"],
        :SCRIPT => @conf["script"],
        :SERVER => server,
        :STATUS => :NONE,
        :USER => @conf["user"],
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
          log(:ERROR, "#{job[:SERVER]}::#{e}")
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

    log(:STDOUTONLY, "Wrote results to #{@outfile}")
  end

private

  def run_command(server, user=ENV["USER"], pcond=nil, command="id", background=false, root=false, script=nil)
    log(:VERBOSE,"#{server}::Connecting")
    command = nil
    sudo = root ? "sudo " : ""
    if background
      nohup = "nohup "
      nohup_end = " &"
    else
      nohup = nohup_end = ""
    end
    Net::SSH.start(server, user) do |ssh|
      exec_command = nil
      if script
        log(:VERBOSE, "#{server}::Using script #{script} (command will be overwritten)")
        basename = File.basename(script)
        remote_dir = "./scripts"
        log(:DEBUG, "#{server}::Creating #{remote_dir}")
        ssh.exec!("test -d #{remote_dir} || mkdir #{remote_dir}")
        log(:DEBUG, "Uploading file #{script} => #{remote_dir}/#{basename}")
        ssh.scp.upload!(script, "#{remote_dir}/#{basename}")
        log(:DEBUG, "Set permissions #{remote_dir}/#{basename} => 0750")
        ssh.exec!("chmod 755 #{remote_dir}/#{basename}")
        command = "#{remote_dir}/#{basename}"
      end

      # Exit the job if pcond file exists on the server
      if pcond
        add = "test -f #{pcond} && echo Precondition #{pcond} exists && exit 1"
        command = "#{add}; #{command}"
      end

      exec_command = "#{nohup}#{sudo}sh -c \"#{command}\"#{nohup_end}"

      log(:VERBOSE, "#{server}::Executing #{exec_command}")
      ssh.exec!(exec_command) do |channel, stream, data|
        log(:OUT, "#{server}::#{data}") unless @conf["silent"]
        if background
          # Give time to attach tty to background
          sleep(3)
          return
        end
      end
    end
  end

  def upload_script(server, user, script, remote_dir)
    Net::SSH.start(server, user) do |ssh|
      #sftp.mkdir!(remote_dir)
    end
  end

  def run_job(job)
    server = job[:SERVER]
    command = job[:COMMAND]
    pcond = job[:PRECONDITION]

    log(:VERBOSE, "#{server}::Running job #{job}")
    if File.exists?("#{server}.ignore")
      log(:INFO, "#{server}::Ignoring this server")
    else
      run_command server, job[:USER], pcond, command, job[:BACKGROUND], job[:ROOT], job[:SCRIPT]
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

    timestamp = @conf["timestamp"] ? "#{Time.now}::" : ""
    message = "#{timestamp}#{severity}::#{message}"

    @log_mutex.synchronize do
      puts message
      if @outfile and severity != :STDOUTONLY
        open(@outfile, @outfile_mode) do |f|
          f.puts message
          @outfile_mode = "a"
        end
      end
    end
  end
end

begin
  opts = GetoptLong.new(
    [ "--background", "-b", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--command", "-c", GetoptLong::REQUIRED_ARGUMENT ],
    [ "--debug", "-d", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--name", "-n", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--outdir", "-o", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--parallel", "-p", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--precondition", "-P", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--root", "-r", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--script", "-s", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--silent", "-S", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--timestamp", "-t", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--user", "-u", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--verbose", "-v", GetoptLong::OPTIONAL_ARGUMENT ],
  )

  Rubyfy.new(opts).run
end
