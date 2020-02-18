const config = require('config');
const csv = require('csv-parser');
const fs = require('fs');
const { Client } = require('@elastic/elasticsearch');
const indexName = config.get('elasticsearch.index_name');
const bulkSize = 20000;
const sleep = async => new Promise(resolve => setTimeout(resolve, 2000));

async function run () {
    // Create Elasticsearch client
    const client = new Client({ node: config.get('elasticsearch.uri') });

    // Create indice with specific mapping for location that is a geopoint in the format "lat,long" with lat lon : float
    client.indices.create({ 
         index: indexName, 
         body : {
            "mappings": {
                "properties": {
                    "location": {
                        "type": "geo_point"
                    }
                }        
            }
        }
    }, (err, resp) => {
        if (err) console.trace(err.message);
    });

    let offenses =[];
    // Read CSV file
    fs.createReadStream('dataset/dans-ma-rue.csv')
        .pipe(csv({
            separator: ';'
        }))
        .on('data', (data) => { 
            //Will construct each offenses with a line of csv
            offenses.push({
                ["@timestamp"] : data.DATEDECL,
                object_id : data.OBJECTID,
                annee_declaration : data["ANNEE DECLARATION"],
                mois_declaration : data["MOIS DECLARATION"],
                type : data.TYPE,
                sous_type : data.SOUSTYPE,
                code_postal : data.CODE_POSTAL,
                ville : data.VILLE,
                arrondissement : data.ARRONDISSEMENT,
                prefixe : data.PREFIXE,
                intervenant : data.INTERVENANT,
                conseil_de_quartier : data["MOIS DECLARATION"],
                location : data.geo_point_2d
            });
        })
        .on('end',async  () => {
            //Because we have too many offenses, we need to make chunk for bulk update - memory problems
            //But making too many bulks can cause 429 error - too many requests - return on some bulk queries, so we need to delay each bulk request
            let index =0;
            while(index <= offenses.length){
                offensesBulk = offenses.slice(index, index+bulkSize);
                index += bulkSize;
                
                await sleep();

                client.bulk(createBulkInsertQuery(offensesBulk), (err, resp) => {
                    if (err) console.trace("error for bulk insert query is" + err.message);
                    else console.log(`Inserted ${resp.body.items.length} offenses`);
                    
                  });
                console.log('Terminated!');

            }
            client.close();
            
        });
}

// Fonction utilitaire permettant de formatter les données pour l'insertion "bulk" dans elastic
function createBulkInsertQuery(offenses) {

    //here we create the body we need to insert data in elastic with the data we created before.
    const body = offenses.reduce((acc, offense) => {
      const { annee_declaration, mois_declaration, type, sous_type , code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location} = offense;
      const timestamp = offense["@timestamp"];
      acc.push({ index: { _index: indexName, _type: '_doc', _id: offense.object_id } })
      acc.push({ ["@timestamp"]:timestamp, annee_declaration, mois_declaration, type, sous_type , code_postal, ville, arrondissement, prefixe, intervenant, conseil_de_quartier, location})
      return acc
    }, []);
  
    return { body };
}
  

run().catch(console.error);