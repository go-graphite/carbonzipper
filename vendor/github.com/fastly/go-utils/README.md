DEPRECATED 
========== 

This repository is demarcated as deprecated. Please don't use it or any of its
packages for new projects, and please consider migrating away from it in
existing projects. Bring questions or concerns to the next internal Go Salon.

go-utils
========
utils for go.

[![Build Status](https://secure.travis-ci.org/fastly/go-utils.png)](http://travis-ci.org/fastly/go-utils)

common
------
An experimental package for functions detecting commonalities between inputs.

debug
-----
A small package with a global variable to turn on or off debugging across all packages
that import debug.

executable
----------
Reports on the status of the running executable, including what directory its
binary was run from and whether any sibling processes are running.

ganglia
-------
Contains wrapper functions for go-gmetric.

instrumentation
---------------
Contains functions for Go runtime introspection.

lifecycle
---------
Provides for clean daemon shutdown after receiving one or more signals.

privsep
-------
Library for implementing privilege-separated processes which can communicate
with each other.

server
------
A package for managing listening sockets. Hides details of closing listening sockets
when shutting a server down.

stopper
-------
A utility interface for stopping channels / functions / anything in a clean manner.

strftime
--------
Pure-Go and system native (via cgo) implementations of POSIX strftime(3).

suppress
--------
Contains utility functions to suppress repeated function calls into one aggregate call.

tls
---
A package that contains functions for loading tls certs and whatnot.

vlog
----
A package that enables or disables verbose logging for any package that imports vlog.
