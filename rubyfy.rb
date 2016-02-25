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

$opts = Hash.new

class Rubyfy
  def initialize
    @log_mutex = Mutex.new
    @outfile = nil
    @outfile_mode = "w"

    $opts["verbose"] = true if $opts["debug"]

    # Read first config found
    ["#{ENV["HOME"]}/.rubyfy.json", "rubyfy.json"].each do |conf_path|
      if File.exists?(conf_path)
        log(:VERBOSE, "Reading #{conf_path}")
        conf_json = JSON.parse(File.read(conf_path))
        log(:VERBOSE, conf_json)
        conf_json.each do |opt, arg|
          $opts[opt] = arg unless $opts[opt]
        end
        break
      end
    end

    # Needed a 2nd time (as we read the config file)
    $opts["verbose"] = true if $opts["debug"]

    # Set defaults of values if not set
    $opts["parallel"] = 1 unless $opts["parallel"]
    $opts["user"] = ENV["USER"] unless $opts["user"]

    # Dealing where to write the output to
    $opts["outdir"] = "./out" unless $opts["outdir"]
    $opts["name"] = "#{ENV["USER"]}" unless $opts["name"]
    @outfile = "#{$opts["outdir"]}/#{$opts["name"]}"
    FileUtils.mkdir_p($opts["outdir"]) unless File.directory?($opts["outdir"])

    log(:DEBUG, $opts)
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
        :BACKGROUND => $opts["background"],
        :COMMAND => $opts["command"],
        :PRECONDITION => $opts["precondition"],
        :ROOT => $opts["root"],
        :SCRIPT => $opts["script"],
        :SCRIPTARGUMENTS => $opts["scriptarguments"],
        :DOWNLOAD => $opts["download"],
        :SERVER => server,
        :STATUS => :NONE,
        :USER => $opts["user"],
      }
      jobs << job
      work_q.push(job)
    end

    parallel = $opts["parallel"].to_i

    threads = (1..parallel).map do
      Thread.new do
        begin
          while job = work_q.pop(true)
            run_job(job)
          end
        rescue ThreadError => e
        rescue => e
          log(:ERROR, "#{job[:SERVER]}::#{__callee__}::#{e.message}")
          log(:ERROR, "#{job[:SERVER]}::#{__callee__}::#{e.inspect}")
          log(:ERROR, "#{job[:SERVER]}::#{__callee__}::#{e}")
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

  def run_command(server, user=ENV["USER"], pcond=nil, command="id", background=false, root=false, script=nil, scriptarguments=nil, download=nil)
    log(:VERBOSE,"#{server}::Connecting")
    sudo = root ? "sudo " : ""
    if background
      nohup = "nohup "
      nohup_end = " &"
    else
      nohup = nohup_end = ""
    end
    Net::SSH.start(server, user) do |ssh|
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
        command += " #{scriptarguments}" unless scriptarguments.nil?
      end

      # Exit the job if pcond file exists on the server
      if pcond
        add = "test -f #{pcond} && echo Precondition #{pcond} exists && exit 1"
        command = "#{add}; #{command}"
      end

      exec_command = "#{nohup}#{sudo}sh -c \"#{command}\"#{nohup_end}"

      log(:VERBOSE, "#{server}::Executing #{exec_command}")
      ssh.exec!(exec_command) do |channel, stream, data|
        log(:OUT, "#{server}::#{data}") unless $opts["silent"]
        if background
          # Give time to attach tty to background
          sleep(3)
          return
        end
      end

      if download
        log(:VERBOSE, "#{server}::Downloading #{download} to file #{server}")
        ssh.scp.download!(download, server)
      end
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
      run_command server, job[:USER], pcond, command, job[:BACKGROUND], job[:ROOT], job[:SCRIPT], job[:SCRIPTARGUMENTS], job[:DOWNLOAD]
    end
    job[:STATUS] = :OK

  rescue ::Exception => e
    log(:ERROR, "#{server}::#{__callee__}::#{e.message}")
    log(:ERROR, "#{server}::#{__callee__}::#{e.inspect}")
    log(:ERROR, "#{server}::#{__callee__}::#{e}")
  end

  def http_get(uri_str, content_type="application/json")
    uri = URI.parse(uri_str)
    req = Net::HTTP::Get.new(uri.path)
    req.[]=("Accept", content_type)
    http = Net::HTTP.new(uri.host, uri.port)
    http.request(req).body
  end

  def log(severity, message)
    return if severity == :VERBOSE and not $opts["verbose"]
    return if severity == :DEBUG and not $opts["debug"]

    timestamp = $opts["timestamp"] ? "#{Time.now}::" : ""
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
    [ "--scriptarguments", "-a", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--download", "-D", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--silent", "-S", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--timestamp", "-t", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--user", "-u", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--verbose", "-v", GetoptLong::OPTIONAL_ARGUMENT ],
    [ "--help", "-h", GetoptLong::OPTIONAL_ARGUMENT ],
  )

  opts.each { |opt, arg| opt.sub!(/^-+/, ''); $opts[opt] = arg }

  if $opts["help"]
    puts <<-END
    -a, --scriptarguments <args..>  Arguments for -s
    -c, --command <command>         Command to run remotely
    -d, --debug                     Enable debug output (implies verbose)
    -D, --download <path>           Download file from remote path after execution
    -h, --help                      Print this help
    -n, --name <name>               Job name (default: $USER)
    -o, --outdir <dir>              Directory to store output files (default: ~/out)
    -p, --parallel <num>            Amount of parallel SSH connections (default: 5)
    -P, --precondition <path>       Only run command if file in path doesn't exist remotely
    -r, --root                      Run specified command as user root (via sudo)
    -s, --script <script.sh>        Upload the script to the remote server and run it
    -S, --silent                    Silent mode
    -t, --timestamp                 Include timestamp in log output
    -u, --user <user>               Login to remote server as a specific user (default: $USER)
    -v, --verbose                   Enable verbose output

    Examples:
      Run command "hostname" on server foo.example.com
          echo foo.example.com | ./srun -c hostname

      Run command "id" on the specified servers, store output in file "jobname"
          echo {foo,bar,baz}.example.com | ./srun -p 2 -r -c id -n jobname

      Upload script "test.sh" to the server, run it, and download file /tmp/test
      afterwards to file foo.example.com.test.
          echo foo.example.com | ./srun -s test.sh -D /tmp/test
    END
    exit(0)
  end

  Rubyfy.new.run
end
