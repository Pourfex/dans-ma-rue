const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.count = async (client, from, to, callback) => {
    from += "T00:00:00"; //include this day in timestamp
    to += "T00:00:00" //excluse this day in timestamp

    //We ask for query count on the timestamp
    const res = await client.count({
        index: indexName,
        body: {
            "query": {
              "range": {
                "@timestamp": {
                  "time_zone": "+01:00",        
                  "gte": from, 
                  "lte": to                 
                }
              }
            }
          }
    });

    //we process the client res to make our own response
    callback({
        "count" : res.body.count
    });
}

exports.countAround = async (client, lat, lon, radius, callback) => {

        //we ask for query count using geo-point
        const res = await client.count({
            index: indexName,
            body: {
                "query": {
                    "bool" : {
                        "must" : {
                            "match_all" : {}
                        },
                        "filter" : {
                            "geo_distance" : {
                                "distance" : radius,
                                "location" : {
                                    "lat" : lat,
                                    "lon" : lon
                                }
                            }
                        }
                    }
                }
            }
        });

     //we process the client res to make our own response
     callback({
        "count" : res.body.count
    });
}


