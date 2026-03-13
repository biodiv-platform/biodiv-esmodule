# ElasticSearch 9.0

Setting up initial observation index after starting elasticsearch instance

Run below query in Kibana before reindexing

**Note:** Type names have been removed in Elasticsearch 8+. The mapping structure has been updated accordingly.

```
PUT observation
{
  "mappings": {
    "properties": {
        "acceptednameid": {
          "type": "long"
        },
        "accessrights": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "agreeterms": {
          "type": "boolean"
        },
        "all": {
          "type": "text"
        },
        "authorid": {
          "type": "long"
        },
        "authorname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "authorprofilepic": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "basisofrecord": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "catalognumber": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "checklistannotations": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "classificationid": {
          "type": "long"
        },
        "createdon": {
          "type": "date",
          "copy_to": [
            "all"
          ]
        },
        "custom_fields": {
          "properties": {
            "5": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "6": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "14215782": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "14215784": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "14215785": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "14215786": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "14215787": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "14215788": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "14215789": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            },
            "14215790": {
              "properties": {
                "key": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                },
                "value": {
                  "type": "text",
                  "fields": {
                    "keyword": {
                      "type": "keyword",
                      "ignore_above": 256
                    }
                  }
                }
              }
            }
          }
        },
        "datasetid": {
          "type": "long"
        },
        "externaldatasetkey": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "externalid": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "externalurl": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "featurecount": {
          "type": "long"
        },
        "featuredgroups": {
          "type": "long"
        },
        "featurednotes": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "flagcount": {
          "type": "long"
        },
        "fromdate": {
          "type": "date",
          "copy_to": [
            "all"
          ]
        },
        "frommonth": {
          "type": "long",
          "copy_to": [
            "all"
          ]
        },
        "geoprivacy": {
          "type": "boolean"
        },
        "habitatid": {
          "type": "long"
        },
        "habitatname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "id": {
          "type": "long"
        },
        "imageresource": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "informationwithheld": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "ischecklist": {
          "type": "boolean"
        },
        "isdeleted": {
          "type": "boolean"
        },
        "islocked": {
          "type": "boolean"
        },
        "isshowable": {
          "type": "boolean"
        },
        "languageid": {
          "type": "long"
        },
        "languagename": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "lastcrawled": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "lastinterpreted": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "lastrevised": {
          "type": "date",
          "copy_to": [
            "all"
          ]
        },
        "latitude": {
          "type": "float",
          "copy_to": [
            "all"
          ]
        },
        "licenseid": {
          "type": "long"
        },
        "licensename": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "location": {
          "type": "geo_point"
        },
        "locationaccuracy": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "locationscale": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "longitude": {
          "type": "float"
        },
        "maxvotedrecoid": {
          "type": "long"
        },
        "name": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "nomedia": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "noofaudio": {
          "type": "long"
        },
        "noofidentifications": {
          "type": "long"
        },
        "noofimages": {
          "type": "long"
        },
        "noofvideos": {
          "type": "long"
        },
        "notes": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "observationlikes": {
          "type": "nested",
          "properties": {
            "icon": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "name": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "rater_id": {
              "type": "long"
            },
            "rating_ref": {
              "type": "long"
            }
          }
        },
        "originalauthor": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "path": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ],
          "analyzer": "underscore_tokenizer"
        },
        "placename": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "position": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "protocol": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "publishingcountry": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "rank": {
          "type": "long"
        },
        "rating": {
          "type": "long"
        },
        "reprimageid": {
          "type": "long"
        },
        "reversegeocodedname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "sourceid": {
          "type": "long"
        },
        "speciesgroupid": {
          "type": "long"
        },
        "speciesgroupname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "status": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "taxonconceptid": {
          "type": "long"
        },
        "taxonomycanonicalform": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "thumbnail": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "todate": {
          "type": "date",
          "copy_to": [
            "all"
          ]
        },
        "topology": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "traits": {
          "properties": {
            "trait_10": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "trait_11": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "trait_12": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "trait_13": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "trait_15": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "trait_8": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            },
            "trait_9": {
              "type": "text",
              "fields": {
                "keyword": {
                  "type": "keyword",
                  "ignore_above": 256
                }
              }
            }
          }
        },
        "traits_json": {
          "type": "object"
        },
        "traits_season": {
          "type": "object"
        },
        "urlresource": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "usergroupid": {
          "type": "long"
        },
        "usergroupname": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          },
          "copy_to": [
            "all"
          ]
        },
        "version": {
          "type": "long"
        },
        "viacode": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "viaid": {
          "type": "text",
          "fields": {
            "keyword": {
              "type": "keyword",
              "ignore_above": 256
            }
          }
        },
        "visitcount": {
          "type": "long"
        }
      }
  },
  "settings": {
    "index": {
      "analysis": {
        "analyzer": {
          "underscore_tokenizer": {
            "tokenizer": "underscore_tokenizer"
          }
        },
        "tokenizer": {
          "underscore_tokenizer": {
            "pattern": "_",
            "type": "simple_pattern_split"
          }
        }
      }
    }
  }
}
```
