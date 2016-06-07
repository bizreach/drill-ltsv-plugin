drill-ltsv-plugin
====
Apache Drill plugin for [LTSV (Labeled Tab-separated Values)](http://ltsv.org/) files.


Installation
----

Download `drill-ltsv-plugin-VERSION.jar` from the [release page](https://github.com/bizreach/drill-ltsv-plugin/releases) and put it into `DRILL_HOME/jars/3rdparty`.

Add ltsv format setting to the storage configuration as below:

```javascript
  "formats": {
    "ltsv": {
      "type": "ltsv",
      "extensions": [
        "ltsv"
      ]
    },
    ...
  }
```

Then you can query `*.ltsv` files on Apache Drill.