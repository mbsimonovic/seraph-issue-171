const db = require("seraph")({
    server: "http://localhost:7474",
    user: "neo4j",
    pass: "neo4j"
});

const when = require('when');
const bunyan = require('bunyan');
const fs = require('fs');
const glob = require("glob")

const log = bunyan.createLogger({name: "seraph-bug", module: "storage/neo4j"});

function create_index() {
    var deferred = when.defer()
    db.index.createIfNone('Protein', 'iid', function (err, index) {
        if (err) {
            log.error(err, 'create_schema - failed to create iid index for Protein')
            deferred.reject('failed to create iid index for Protein: ' + err)
            return;
        }
        deferred.resolve()
    })
    return deferred.promise
}

function save(proteins, abundances) {
    var numAbundances = 0
    for (var p in abundances) {
        if (abundances.hasOwnProperty(p)) {
            numAbundances += abundances[p].length
        }
    }

    log.info('importing %s proteins and %s abundances', proteins.length, numAbundances)

    var d = when.defer()
    var txn = db.batch();

    proteins.forEach(function (p) {
        var node = txn.save(p);
        txn.label(node, "Protein");
        if (abundances[p.eid] && abundances[p.eid].length > 0) {
            abundances[p.eid].forEach(function (el) {
                var abundance = txn.save({"value": el.value, "rank": el.rank});
                txn.label(abundance, "Abundance");
                txn.relate(node, el.tissue, abundance);
            })
        }
    })
    txn.commit(function (err, results) {
        if (err) {
            log.error(err, 'import_proteins - FAILED')
            var e = Error("TRANSACTION FAILED: " + err.message);
            e.results = results;
            d.reject(e);
            return
        }
        d.resolve();
    })
    return d.promise
}

function import_data(file, abundances_dir) {
    log.info("proteins from %s", file);
    var speciesId = /\/?(\d+)\-proteins.txt/.exec(file)[1]
    log.info('species: %s', speciesId)
    var proteins = parseProteins(fs.readFileSync(file, {'encoding': 'utf8'}));
    log.debug('%s protein records from %s', proteins.length, file)
    var abundances = loadAbundances(speciesId, abundances_dir);

    return save(proteins, abundances)
}

function parseProteins(contents) {
    var proteins = []
    contents.split('\n').forEach(function (line) {
        if (line.trim() == 0) {
            return
        }
        var rec = line.split('\t');
        proteins.push({"iid": parseInt(rec[0]), "eid": rec[1], "name": rec[2]})
    })
    return proteins
}

function parseDataset(contents) {
    var dataset = {"abundances": []}
    var records = contents.split('\n');
    for (var i = 0; i < records.length && records[i].indexOf('#') == 0; i++) {
        if (records[i].indexOf('organ:') != -1) {
            dataset.organ = records[i].match(/organ\:\s+([A-Z_]+)/)[1]
        }
    }
    for (/*i from previous loop*/; i < records.length; i++) {
        var r = records[i].trim().split('\t');
        if (r.length < 2) {
            continue
        }
        dataset.abundances.push({iid: parseInt(r[0]), eid: r[1], value: parseFloat(r[2])})
    }
    dataset.numAbundances = dataset.abundances.length
    return dataset
}

function loadAbundances(speciesId, abundances_dir) {
    var abundances = {}
    var abundanceFiles = glob.sync(abundances_dir + '/' + speciesId + '-*.txt')
    log.debug('abundance files found: %s', abundanceFiles)
    abundanceFiles.forEach(function (datasetFile) {
        log.info('reading %s abundances from %s', speciesId, datasetFile)
        var counter = 0
        var dataset = parseDataset(fs.readFileSync(datasetFile, {'encoding': 'utf8'}));

        //TODO refactor to appendAbundances(abundances, dataset.abundances)
        var outOf = '/' + String(dataset.numAbundances);
        for (var i = 0; i < dataset.abundances.length; i++) {
            var p = dataset.abundances[i];
            if (!abundances.hasOwnProperty(p.eid)) {
                abundances[p.eid] = []
            }
            counter++
            abundances[p.eid].push({"tissue": dataset.organ, value: p.value, rank: String(i + 1) + outOf})
        }
        log.info('%s abundances read from %s', counter, datasetFile)
    })
    return abundances;
}


log.level('debug')

create_index().then(function () {
    log.info("schema created")
    import_data('data/proteins/10090-proteins.txt', 'data/abundances').then(function () {
        log.info("import complete")
    }, function (err) {
        log.error(err, "failed to import!")
    })
}, function (err) {
    log.error(err, "failed to create schema!")
})


