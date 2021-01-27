
// On utilise un driver pour pouvoir utiliser la base cassandra
var cassandra = require('cassandra-driver');

// les "\xib[xxm" dans les "console.log()" servent pour la couleur dans la console.

// On execute toute les fonction de manière asynnchrone
// pour que toute les données restent cohérentes
// et que la connection se fasse avant une quelconque requête.
(async () => {

    // On créé un client cassandra qui va se connecter en local
    const client = new cassandra.Client({ contactPoints: ['127.0.0.1'], localDataCenter: 'datacenter1'});

    console.log('trying to connect to cassandra server...');

    // Connection à cassandra
    await new Promise(resolve => {
        client.connect(err => {

            if (err) {
                console.error('\x1b[31m%s\x1b[0m', 'error during connexion...')
                return console.error(err);
            } else

                console.log('\x1b[32m%s\x1b[0m', `Connected to the cluster with host ${client.hosts.keys()} \n`);

            resolve();
        });
    });


    // query pour la création d'un keyspace
    let query = `create keyspace if not exists xx_keyspace_js with replication = {'class': 'SimpleStrategy', 'replication_factor' : 1}`;

    console.log('trying to create a keyspaces...');

    // Création d'une keyspace
    await new Promise(resolve => {
        client.execute(query, (err, result) => {

            console.log('\x1b[1m\x1b[33m%s\x1b[0m', query);

            if (err) {
                console.error('\x1b[31m%s\x1b[0m', 'error during creation of keyspace...')
                console.error(err);
            } else {
                console.log('\x1b[32m%s\x1b[0m', `keyspace successfully created or already existing \n`);
            }

            resolve();

        });
    });

    // query pour l'affichage des keyspaces existants
    query = `select * from system_schema.keyspaces`;

    console.log('trying to list keyspaces');

    // affichage des keyspaces
    await new Promise(resolve => {
        client.execute(query, (err, result) => {

            console.log('\x1b[1m\x1b[33m%s\x1b[0m', query);

            if (err) {
                console.error('\x1b[31m%s\x1b[0m', 'error during listing of keyspaces...');
                console.error(err);
            } else {
                console.log('\x1b[32m%s\x1b[0m', `keyspace successfully listed`);

                result.rows.forEach( row => {
                    console.log('\x1b[36m%s\x1b[0m', row.keyspace_name);
                })

                console.log('\n');
            }

            resolve();

        });
    });

    // query pour la création d'une table
    query = `create table if not exists xx_keyspace_js.xx_table_js(id uuid, name text, primary key ((id)) );`;

    console.log('trying to create a table...');

    // création de la table
    await new Promise(resolve => {
        client.execute(query, (err, result) => {

            console.log('\x1b[1m\x1b[33m%s\x1b[0m', query);

            if (err) {
                console.error('\x1b[31m%s\x1b[0m', `'error during creation of table...'`);
                console.error(err);
            } else {
                console.log('\x1b[32m%s\x1b[0m', `table successfully created \n`);

            }

            resolve();

        });
    });

    // query pour vider la table
    query = `truncate xx_keyspace_js.xx_table_js`;

    console.log('trying to truncate the table...');

    // on vide la table
    await new Promise(resolve => {
        client.execute(query, (err, result) => {

            console.log('\x1b[1m\x1b[33m%s\x1b[0m', query);

            if (err) {
                console.error('\x1b[31m%s\x1b[0m', 'error during truncation of table...');
                console.error(err);
            } else {
                console.log('\x1b[32m%s\x1b[0m', `table successfully truncate \n`);
            }

            resolve();

        });
    });

    // query pour l'insertion de données
    query = `insert into xx_keyspace_js.xx_table_js(id,name) values (uuid(), 'insert from node.js');`;

    console.log('trying to insert 1/3...');

    // Insertion de données
    await new Promise(resolve => {
        client.execute(query, (err, result) => {

            console.log('\x1b[1m\x1b[33m%s\x1b[0m', query);

            if (err) {
                console.error('\x1b[31m%s\x1b[0m', `'error during insertion 1/3...'`);
                console.error(err);
            } else {
                console.log('\x1b[32m%s\x1b[0m', `insert successfully done 1/3 \n`);
            }

            resolve();

        });
    });

    // query pour l'insertion de données
    query = `insert into xx_keyspace_js.xx_table_js(id,name) values (uuid(), 'second insertion');`;

    console.log('trying to insert 2/3...');

    // Insertion de données
    await new Promise(resolve => {
        client.execute(query, (err, result) => {

            console.log('\x1b[1m\x1b[33m%s\x1b[0m', query);

            if (err) {
                console.error('\x1b[31m%s\x1b[0m', `'error during insertion 2/3...'`);
                console.error(err);
            } else {
                console.log('\x1b[32m%s\x1b[0m', `insert successfully done 2/3 \n`);
            }

            resolve();

        });
    });

    // query pour l'insertion de données
    query = `insert into xx_keyspace_js.xx_table_js(id,name) values (uuid(), 'third insertion');`;

    console.log('trying to insert 3/3...');

    // Insertion de données
    await new Promise(resolve => {
        client.execute(query, (err, result) => {

            console.log('\x1b[1m\x1b[33m%s\x1b[0m', query);

            if (err) {
                console.error('\x1b[31m%s\x1b[0m', `'error during insertion 3/3...'`);
                console.error(err);
            } else {
                console.log('\x1b[32m%s\x1b[0m', `insert successfully done 3/3 \n`);
            }

            resolve();

        });
    });

    // query pour l'affichage de la table
    query = `select * from xx_keyspace_js.xx_table_js`;

    console.log('trying to fetch data...')

    // affichage de la table
    await new Promise(resolve => {
        client.execute(query, (err, result) => {

            console.log('\x1b[1m\x1b[33m%s\x1b[0m', query);

            if (err) {
                console.error('\x1b[31m%s\x1b[0m', `'error during fetch...'`);
                console.error(err);
            } else {
                console.log('\x1b[32m%s\x1b[0m', `data successfully fetch`);

                result.rows.forEach( row => {

                    console.log('\x1b[36m%s\x1b[0m', row);

                })
                console.log('\n');
            }

            resolve();

        });
    });

    await client.shutdown();

    return process.exit(0);

})();

