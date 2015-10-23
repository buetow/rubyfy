Rubyfy
======

Tested on 

* Fedora 22 / Ruby 2.2.3
* Mac OS X / MacPorts Ruby 2.2.0

Example usage:

```
# Run command 'hostname' on server foo.example.com
./rubyfy.rb -c 'hostname' <<< foo.example.com

# Run command 'id' as root (via sudo) on all servers listed in the list file
# Do it on 10 servers in parallel
./rubyfy.rb --parallel 10 --root --command 'id' < serverlist.txt

# Run a fancy script in background on 50 servers in parallel
./rubyfy.rb -p 50 -r -b -c '/usr/local/scripts/fancy.zsh' < serverlist.txt

# Grep for specific process on both servers and write output to ./out/grep.txt
echo {foo,bar}.example.com | ./rubyfy.rb -p 10 -c 'pgrep -lf httpd' -n grep.txt

# Reboot server only if file /var/run/maintenance.lock does NOT exist!
./rubyfy.rb --root --command reboot --precondition /var/run/maintenance.lock
```


