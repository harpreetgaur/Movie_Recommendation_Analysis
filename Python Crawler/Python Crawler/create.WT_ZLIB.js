Create Database with zlib:
db.createCollection( "test", {storageEngine:{wiredTiger:{configString:'block_compressor=zlib'}}} );

Remove all document of a collection:
db.user.remove({})

Update all scraped to true/false:
db.na_player_list.updateMany({}, {$set: {"scraped": true}})