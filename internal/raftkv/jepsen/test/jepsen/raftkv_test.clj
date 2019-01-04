; Copyright (c) 2015 Western Digital Corporation or its affiliates.  All rights reserved.
; SPDX-License-Identifier: MIT

(ns jepsen.raftkv-test
  (:require [clojure.test :refer :all]
            [jepsen.core :refer [run!]]
            [jepsen.raftkv :as raftkv]))

(def version "0.0.1")

(deftest basic-test
  (is (:valid? (:results (run! (raftkv/basic-test version))))))
