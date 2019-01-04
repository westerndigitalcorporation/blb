; Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
; SPDX-License-Identifier: MIT

(ns jepsen.raftkv
  "Tests for RaftKV"
  (:require [clojure.tools.logging :refer :all]
            [clojure.core.reducers :as r]
            [clojure.java.io :as io]
            [clojure.string :as str]
            [clojure.pprint :refer [pprint]]
            [clj-http.client :as http]
            [knossos.op :as op]
            [jepsen [client :as client]
                    [core :as jepsen]
                    [db :as db]
                    [tests :as tests]
                    [control :as c :refer [|]]
                    [checker :as checker]
                    [nemesis :as nemesis]
                    [generator :as gen]
                    [util :refer [timeout meh]]]
            [jepsen.model     :as model]
            [jepsen.control.util :as cu]
            [jepsen.control.net :as cn]
            [jepsen.os.debian :as debian]
            [slingshot.slingshot :refer [try+]]))

(def pidfile "/var/run/raftkv.pid")
(def port 8080)
(def data-dir "/tmp/raft")
(def binary "/bin/raftkv")
(def log-file "/var/log/raftkv.log")

(defn start-raftkv!
  [node test]
  (let [nodes (set (:nodes test))
        members (str/join "," (doall (map #(str (name %) ":" port) nodes)))
        bin-file "/bin/raftkv"]
    (info "making directory" data-dir)
    (c/exec :mkdir :-p data-dir)
    (info "start" nodes members bin-file)
    (c/exec :start-stop-daemon :--start
      :--background
      :--make-pidfile
      :--pidfile pidfile
      :--exec binary
      :--no-close
      :--
      :-data-dir data-dir
      :-addr (str (name node) ":" port)
      :-members members
      :> log-file
      (c/lit "2>&1"))
    (info "sleeping")
    (Thread/sleep 3000)
    (info "done")
))

(defn db
  "Sets up and tears down RaftKV"
  [version]
  (reify db/DB
    (setup! [_ test node]
      (start-raftkv! node test))

    (teardown! [_ test node]
      (c/su
        (meh (c/exec :killall :-9 :raftkv))
        (c/exec :rm :-rf pidfile data-dir))
      (info node "raftkv nuked"))))

(defn raftkv-get
  [key-url]
  (Integer. (:body (http/get key-url {:force-redirects true :max-redirects 5}))))

(defn raftkv-put
  [key-url value]
  (http/put key-url {:body (str value) :force-redirects true :max-redirects 5}))

(defn raftkv-cas
  [key-url old-value new-value]
  (http/put (str key-url "?cas=" old-value) {:body (str new-value) :force-redirects true :max-redirects 5}))


(defrecord CASClient [k client]
  client/Client
  (setup! [this test node]
    (let [client (str "http://" (name node) ":" port "/" k)]
      (info "client:", client)
      (assoc this :client client)))

  (invoke! [this test op]
    (case (:f op)
      :read (try (let [value (raftkv-get client)]
                   (assoc op :type :ok :value value))
             (catch Exception e
               (warn "Read failed, client:", client)
               (assoc op :type :fail)))

      :write (try+ (do (raftkv-put client (:value op))
                      (assoc op :type :ok))

              ; A few failure type RaftKV can return.
              (catch [:status 503] e
                (warn "Put failed because of service unavailable, client:", client)
               (assoc op :type :fail))

              (catch [:status 408] e
                (warn "Put failed due to timeout, client:", client)
               (assoc op :type :info)))

      :cas (try+ (let [[old-value new-value] (:value op)]
                   (raftkv-cas client old-value new-value)
                   (assoc op :type :ok))

              ; A few failure type RaftKV can return.
              (catch [:status 503] e
                (warn "Put failed because of service unavailable, client:", client)
               (assoc op :type :fail))

              (catch [:status 408] e
                (warn "Put failed due to timeout, client:", client)
               (assoc op :type :info))

              (catch [:status 409] e
                (warn "CAS op failed, client:", client)
                (assoc op :type :fail)))))

  (teardown! [_ test]))

(defn cas-client
  "A compare and set register built around a single raftkv node."
  []
  (CASClient. "jepsen" nil))

(defn basic-test
  "A simple test of RaftKV's safety"
  [version]
  (merge tests/noop-test
          {
           :name      "raftkv"
           :db        (db version)
           :os        debian/os
           :model     (model/cas-register)
           :client    (cas-client)
           :nemesis   (nemesis/partition-random-halves)
           :generator (gen/phases
                        (->> gen/cas
                             (gen/delay 1/2)
                             (gen/nemesis
                             (gen/seq
                                (cycle [(gen/sleep 10)
                                        {:type :info :f :start}
                                        (gen/sleep 10)
                                        {:type :info :f :stop}])))
                             (gen/time-limit 60))
                          (gen/nemesis
                            (gen/once {:type :info :f :stop}))
                          (gen/clients
                            (gen/once {:type :invoke :f :read})))}))
