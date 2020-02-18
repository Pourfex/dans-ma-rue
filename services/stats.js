const config = require('config');
const indexName = config.get('elasticsearch.index_name');

exports.statsByArrondissement = async (client, callback) => {
    //we ask for an aggregation on the arrondissement keyword
    const res = await client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs" : {
                "arrondissement" : {
                    "terms": {
                        "field": "arrondissement.keyword",
                        "size" : 21
                    }
                }
            }
        }        
    });

    //We then need to go into buckets to have the wanted results
    aggreg = res.body.aggregations.arrondissement.buckets;

    //we process the client res to make our own response
    callback(
        aggreg
    );
}

exports.statsByType = async (client, callback) => {
    //we ask for an aggregation on type keyword, followed by a inner aggregation on sous_type keyword both with size 5
    const res = await client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs" : {
                "type" : {
                    "terms": {
                        "field": "type.keyword",
                        "size" : 5
                    },
                    "aggs" : {
                        "sous_types" : {
                            "terms": {
                                "field": "sous_type.keyword",
                                "size":5
                            }
                        }
                    }
                }
            }
        }            
    });

    //We then need to go into buckets to have the wanted results in a recursive way
    formatRes = res.body.aggregations.type.buckets.map(bucket => ({ 
        type : bucket.key, 
        count : bucket.doc_count,
        sous_types : bucket.sous_types.buckets.map(bucket => ({ 
            type : bucket.key, 
            count : bucket.doc_count,
        }))
    })); 
   
    //we process the client res to make our own response
    callback(
        formatRes
    );
}

exports.statsByMonth = async (client, callback) => {
    //we ask for an date_histogram aggregatin on timestamp, using calendar_interval (!interval is deprecated), format it and order by count
    const res = await client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs" : {
                "month" : {
                    "date_histogram": {
                        "field": "@timestamp",
                        "calendar_interval" : "month",
                        "format" : "MM/yyyy",
                        "order": {"_count": "desc"}
                    }
                }
            }
        }    
    });
    
    //We take the 10 first of the bucket and format them
    formatRes = res.body.aggregations.month.buckets.splice(0,10).map(bucket => ({
        month : bucket.key_as_string,
        count : bucket.doc_count
    }));
   
    //we process the client res to make our own response
    callback(
        formatRes
    );
}



exports.statsPropreteByArrondissement = async (client, callback) => {
    //we ask for an aggregation on the arrondissement keyword, filtered on the type proprete with a size of 3
    const res = await client.search({
        index: indexName,
        body: {
            "size": 0,
            "aggs" : {
        		"filter" : {
            		"filter" : {
            			"term": { 
            				"type": "propreté" 
            			} 
            		},
            		"aggs" : {
                		"arrondissement" : {
                    		"terms": {
                    			"field": "arrondissement.keyword",
                        		"size" : 3
                			 }
                		}
            		}	
        		}
            }
        }        
    });

    //We then need to go into buckets to have the wanted results, and we format them
    aggreg = res.body.aggregations.filter.arrondissement.buckets.map(bucket => ({
        arrondissement : bucket.key,
        count : bucket.doc_count
    }));
    //we process the client res to make our own response
    callback(
        aggreg
    );
}
