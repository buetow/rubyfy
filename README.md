Rubyfy
======

Tested on 

* Fedora 22 / Ruby 2.2.3
* Mac OS X / MacPorts Ruby 2.2.0

Example usage:

```
cat serverlist.txt | ./rubyfy -p 10 -c 'hostname'

./rubyfy --root --command 'id' <<< foo.example.com
```


