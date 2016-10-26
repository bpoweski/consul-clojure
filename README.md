# consul-clojure, a Consul client for Clojure

[Consul](https://www.consul.io) is an awesome service discovery and configuration provider.


## Changelog

### 0.7.0

 > Consul 0.7 Features
 
 * Consul 0.7 KV transactions courtesy of @pimeys


### 0.6.0

 > This is a **major update** that **may be BREAKING**.

```clojure
[consul-clojure "0.6.0"]
```

* Pulled in the following changes from a fork
* **BREAK**: Dropped use of metadata for tracking various values
* **BREAK**: No more keyword args in favor of maps
* **BREAK**: Dropped the dependency on com.stuartsierra.component and components in favor of fns
* **FIX**: NPE on first :failures increment
* Add leader watch


### 0.1.0

> Initial release

```clojure
[consul-clojure "0.1.0"] ;; initial
```

## Goals

Provide a useful library for building Consul aware Clojure applications.

## Getting Started

First, you'll need to setup a local consul agent.  See [setup instructions](https://www.consul.io/intro/getting-started/install.html).  It's much easier to see what's happening by using the consul-web-ui.

This is the LaunchAgent configuration I use for OSX after installing `consul` and `consul-web-ui`.

```xml
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd"\>
<plist version="1.0">
    <dict>
        <key>KeepAlive</key>
        <true/>
        <key>Label</key>
        <string>hashicorp.consul.server</string>
        <key>ProgramArguments</key>
        <array>
            <string>/usr/local/bin/consul</string>
            <string>agent</string>
            <string>-dev</string>
            <string>-bind=127.0.0.1</string>
            <string>-data-dir=/usr/local/var/consul</string>
            <string>-config-dir=/usr/local/etc/consul.d</string>
            <string>-ui-dir=/usr/local/Cellar/consul/0.6.4/share/consul/web-ui</string>
        </array>
        <key>RunAtLoad</key>
        <true/>
        <key>UserName</key>
        <string>bpoweski</string>
        <key>WorkingDirectory</key>
        <string>/usr/local/var</string>
        <key>StandardErrorPath</key>
        <string>/usr/local/var/log/consul.log</string>
        <key>StandardOutPath</key>
        <string>/usr/local/var/log/consul.log</string>
    </dict>
</plist>
```

### Connections

Given much of the time one is interacting with a local consul agent, the keyword `:local` can be used to assume the defaults.  In those other cases
where someone has gotten "creative" in the deployment of consul or you need to connect to a remote server, you can use a `clj-http-lite` compatible request maps.

```clojure
{:server-name "127.0.0.1" :server-port 8500}
```

### Key/Value store

When using the Key/Value store endpoints directly, one has to contend with a variety of different response formats depending upon the parameters passed.
Rather than continue with this approach, different functions are used instead.

Getting a key:

```clojure
(require '[consul.core :as consul])

(consul/kv-get :local "my-key")
=> ["my-key" nil]
```

But what about the consul index information?

```clojure
(meta (consul/kv-get :local "my-key"))
=> {:x-consul-lastcontact "0", :known-leader true, :modify-index 352}
```

Setting a value:

```clojure
(consul/kv-put :local "my-key" "a")
=> true
```

Want a list of keys?

```clojure
(consul/kv-keys :local "my")
=> #{"my-key"}
```

Don't want a key?

```clojure
(consul/kv-del :local "my-key")
=> true
```

Let's remove more than one:

```clojure
(consul/kv-put :local "key-1" "a")
=> true
(consul/kv-put :local "key-2" nil)
=> true
(consul/kv-keys :local "key")
=> #{"key-1" "key-2"}

(consul/kv-del :local "key" :recurse? true)
=> true
(consul/kv-keys :local "key")
=> #{}
```

## Atomic transactions (available in Consul 0.7)

The txn library allows executing multiple operations in an atomic transaction. All operations
in the txn Consul 0.7 txn documentation are supported https://www.consul.io/docs/agent/http/kv.html

The result of a transaction is always either a list of results, or an exception if the transaction
was not successful.

```clojure
(require '[consul.txn :as txn])

(txn/put :local (fn [tx] (txn/kv-set tx "key" "val")
                         (txn/kv-get tx "key")))

=> ({:create-index 3296,
     :flags 0,
     :key "key",
     :lock-index 0,
     :modify-index 3296,
     :value nil}
    {:create-index 3296,
     :flags 0,
     :key "key",
     :lock-index 0,
     :modify-index 3296,
     :value "val"})
```

If any of the operations fail, the whole transaction fails. Errors are reported in the exception `:errors` key.

Currently supported operations: `kv-set`, `kv-get`, `kv-set-cas`, `kv-lock`, `kv-unlock`, `kv-get-tree`, `kv-check-index`,
`kv-check-session`, `kv-delete`, `kv-delete-tree`, `kv-delete-cas`.

### Agent

The agent consul messages generally are where most applications will interact with Consul.

Return the checks a local agent is managing:

```clojure
(agent-checks :local)
=> {"service:redis-04"
    {:Name "Service redis-shard-1' check",
     :ServiceName "redis-shard-1",
     :Status "warning",
     :CheckID "service:redis-04",
     :Output
     "Could not connect to Redis at 127.0.0.1:6503: Connection refused\n",
     :Notes "",
     :Node "MacBook-Pro.attlocal.net",
     :ServiceID "redis-04"}}
```

List services registered with the agent.

```clojure
(agent-services :local)
=> {"consul"
    {:ID "consul", :Service "consul", :Tags [], :Address "", :Port 8300}}
```

List members the agent sees.

```clojure
(agent-members :local)
=> ({:DelegateMax 4,
     :Name "server.domain.net",
     :Addr "192.168.1.72",
     :ProtocolMin 1,
     :Status 1,
     :ProtocolMax 2,
     :DelegateCur 4,
     :DelegateMin 2,
     :ProtocolCur 2,
     :Tags
     {:bootstrap "1",
      :build "0.5.2:9a9cc934",
      :dc "dc1",
      :port "8300",
      :role "consul",
      :vsn "2",
      :vsn_max "2",
      :vsn_min "1"},
     :Port 8301})
```

Return the local agent configuration.

```clojure
(agent-self :local)
=> ...
```

Put a node into maintenance mode.

```clojure
(agent-maintenance :local true)
=> true
```

Take it out of maintenance mode.

```clojure
(agent-maintenance :local false)
=> true
```

Join a node into a cluster using the RPC address.

```clojure
(agent-join :local "10.1.3.1:8400")
=> true
```

Force leave a node.

```clojure
(agent-force-leave :local "10.1.3.1:8400")
=> true
```

#### Check Management


#### Service Management


### Catalog

### Health Checks

### Access Control Lists

### User Events

### Status

## License

Copyright © 2015 Benjamin Poweski

Distributed under the Eclipse Public License either version 1.0 or (at
your option) any later version.
