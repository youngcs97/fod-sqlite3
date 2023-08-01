const fs = require("fs");
const { parse } = require("csv-parse");

const props = {
    issues: [
        'Application','Application ID',
        'Release','Release ID','Microservice',
        'Instance ID','Kingdom','Category','Severity',
        'Status', 'LocationFull', 'Line Number', 
        'Scan completed date'
    ],
    scans: [
        'Application','Application ID',
        'Release','Release ID',
        'Scan ID','EntitlementUnitsConsumed','IsSubscriptionEntitlement','LOCCount','FileCount','TotalIssues',
        'FixedIssueCount','ExistingIssueCount','ReopenIssueCount','NewIssueCount',
        'Scan Complete Date'
    ]
}
//props.forEach((f)=>{ console.log(`row['${f}'],`) })
//return

const fields = { 
    issues: [
        ["Application","varchar(255)"],
        ["ApplicationID","INT"],
        ["Release","varchar(255)"],
        ["ReleaseID","INT"],
        ["Microservice","varchar(255)"],
        ["Instance","varchar(255)"],
        ["Kingdom","varchar(255)"],
        ["Category","varchar(255)"],
        ["Severity","varchar(255)"],
        ["Status","varchar(255)"],
        ["Location","varchar(1000)"],
        ["Line","INT"],
        ["Completed","varchar(255)"]
    ],
    scans: [
        ["Application","varchar(255)"],
        ["ApplicationID","INT"],
        ["Release","varchar(255)"],
        ["ReleaseID","INT"],
        ["ScanID","varchar(255)"],
        ["AUs","INT"],
        ["Subscription","INT"],
        ["Lines","INT"],
        ["Files","INT"],
        ["Issues","INT"],
        ["Fixed","INT"],
        ["New","INT"],
        ["Existing","INT"],
        ["Reopened","INT"],
        ["Completed","INT"]
    ]
}

function table(name, fields) {
    let cmd = []
    fields.forEach((f)=>{ cmd.push(`${f[0]} ${f[1]}`)})
    return `CREATE TABLE ${name} (${cmd.join(",")})`
}

// Database
function connect() {
    const sqlite3 = require("sqlite3").verbose();
    const file = ":memory:";
    if (fs.existsSync(file)) {
        return new sqlite3.Database(file);
    } else {
        const db = new sqlite3.Database(file, (error) => {
            if (error) {
                return console.error(error.message);
            }
            let cmd = []
            cmd.push(table("rawissues", fields.issues))
            cmd.push(table("rawscans", fields.scans))
            db.exec(cmd.join("; "),(s,e)=>{
                console.log("Connected to the database successfully");
            })
        });
        return db;
    }
}

function command(name, length) {
    let cmd = "?,".repeat(length-1)+"?"
    cmd = `INSERT INTO ${name} VALUES (${cmd})`
    return db.prepare(cmd)
}

function backup() {
    db.serialize(()=> {
        let b = db.backup('./fod.db');
        b.step(-1, function(err) {
            if (err) throw err;
            b.finish(function(err) {
                if (err) throw err;
                console.log("Database saved")
            });
        });
    })
}

function csv(sql, file) {
    db.serialize(()=> {
        const { stringify } = require("csv-stringify");
        const writableStream = fs.createWriteStream(file);
        const stringifier = stringify({ header: true, quote: true });
        db.each(sql, (error, row) => {
        if (error) {
            return console.log(error.message);
        }
        stringifier.write(row);
        });
        stringifier.pipe(writableStream);
        console.log("Finished writing data");
    })
}


// File parsing
function readissues(file) {
    let cmd = command("rawissues", props.issues.length)
    let i = 0;  // row processing counter
    fs.createReadStream(file)
        .pipe(parse({ delimiter: ",", columns: true, quote: "\"" }))
        .on("data", function (row) {
            //console.log(row);
            db.serialize(()=> {
                cmd.run(
                    row['Application'],
                    parseInt(row['Application ID']),
                    row['Release'],
                    parseInt(row['Release ID']),
                    row['Microservice'],
                    row['Instance ID'],
                    row['Kingdom'],
                    row['Category'],
                    row['Severity'],
                    row['Status'],
                    row['LocationFull'],
                    parseInt(row['Line Number']),
                    Date.parse(row['Scan completed date'])
                );
            })
            i++
            if (i % 1000 == 0) { console.log(`issues: ${i}`) }   // feedback every thousand records
        })
        .on("close",()=>{
            db.serialize(()=> {
                db.get("SELECT count(1) as count from rawissues", [], (err, row) => {
                    if (err) { throw err; }
                    console.log(`Total rows processed: ${row.count}`);
                });
                db.exec(`
                CREATE TABLE severity (description varchar(255), rank int);
                INSERT INTO severity (description, rank) values ('Critical',5), ('High',4),('Medium',3),('Low',2),('Info',1),('Best Practice',0);
                `)
                db.exec(`
                CREATE TABLE status (description varchar(255), rank int);
                INSERT INTO status (description, rank) values ('Reopen',4),('Fix Validated',3),('Existing',2),('New',1);
                `)
            })
            db.serialize(()=> {
                db.exec(`
                CREATE VIEW applications AS 
                SELECT Application, ApplicationID, min(Completed) as FirstSeen, max(Completed) as LastSeen,
                    date(min(Completed)/1000, 'unixepoch') as FirstSeenDate, 
                    date(max(Completed)/1000, 'unixepoch') as LastSeenDate, 
                    count(1) as RawIssues
                FROM rawissues
                GROUP BY Application, ApplicationID
                `)
            })
            db.serialize(()=> {
                db.exec(`
                    CREATE TABLE temp as
                    SELECT min(A.rowid) as ID, Application, ApplicationID, Microservice, Location, Kingdom, Category, Instance, 
                        Status, C.rank as StatusRank, Severity, B.rank as SeverityRank, Line,
                        count(1) as RawIssues, min(Completed) as FirstSeen, max(Completed) LastSeen
                    FROM rawissues A 
                        LEFT JOIN severity B on A.Severity=B.description
                        LEFT JOIN status C on A.Status=C.description
                    GROUP BY 2,3,4,5,6,7,8,9,10,11,12,13
                `)
            })
            let di = dedupeissues()
            di.forEach((sql)=>{
                db.serialize(()=> { 
                    db.exec(sql)
                    db.get("SELECT count(1) as count from temp", [], (err, row) => {
                        if (err) { throw err; }
                        console.log(`Issue count: ${row.count}`);
                    });
                })
            })
            db.serialize(()=> {
                db.exec(`
                    CREATE TABLE issues as
                    SELECT *,
                        date(FirstSeen/1000, 'unixepoch') as FirstSeenDate,
                        date(LastSeen/1000, 'unixepoch') as LastSeenDate
                    FROM temp;
                    DROP TABLE temp;
                `)
            })


            backup()
            csv(`select * from issues`, "./issues.csv")
        })
}
function readscans(file) {
    let cmd = command("rawscans", props.scans.length)
    let i = 0;  // row processing counter
    fs.createReadStream(file)
        .pipe(parse({ delimiter: ",", columns: true, quote: "\"" }))
        .on("data", function (row) {
            db.serialize(()=> {
                cmd.run(
                    row['Application'],
                    parseInt(row['Application ID']),
                    row['Release'],
                    parseInt(row['Release ID']),
                    parseInt(Object.values(row)[0]),  // some weird unprinted char in field name -- use ordinal reference instead
                    parseInt(row['EntitlementUnitsConsumed']),
                    parseInt((row['IsSubscriptionEntitlement'].toLowerCase()=='true')?1:0),
                    parseInt(row['LOCCount']),
                    parseInt(row['FileCount']),
                    parseInt(row['TotalIssues']),
                    parseInt(row['FixedIssueCount']),
                    parseInt(row['NewIssueCount']),
                    parseInt(row['ExistingIssueCount']),
                    parseInt(row['ReopenIssueCount']),
                    Date.parse(row['Scan Complete Date'])
                );
            })
            i++
            if (i % 1000 == 0) { console.log(`scans: ${i}`) }   // feedback every thousand records
        })
        .on("close",()=>{
            db.serialize(()=> {
                db.get("SELECT count(1) as count from rawscans", [], (err, row) => {
                    if (err) { throw err; }
                    console.log(`Total rows processed: ${row.count}`);
                });
            })
            db.serialize(()=> {
                db.exec(`
                    CREATE VIEW scans as
                    SELECT *, date(Completed/1000, 'unixepoch') as CompletedDate
                    FROM rawscans
                `)
            })
            backup()
            csv(`select * from scans`, "./scans.csv")
        })
}

function dedupeissues() {
    let cmd = []
    let fields = ['Application', 'ApplicationID', 'Microservice', 'Location', 'Kingdom', 'Category', 'Instance', 'Status', 'Severity','LastSeen']
    let aggregates = ['StatusRank','SeverityRank','LastSeen','ID']
    for (let i=0; i<2; i++) {
        for (let j=7; j<11; j++) {
            let f = fields.slice(0,j)   // copy leading portion of array
            let g = f.slice()   // make the group by sequence
            for (let k=0; k<g.length; k++) {
                g[k]=k+1
            }
            let a = aggregates[j-7] // define aggregate
            let w = f.slice()   // make the where clause
            w.push(a)
            for (let k=0; k<w.length; k++) {
                w[k]=`E.${w[k]} = F.${w[k]}`
            }
            if (j<10) f.push(`max(${a}) as ${a}`)  // add aggregate except last iteration

            let c = `
DELETE FROM temp WHERE ID IN (
    SELECT E.ID FROM temp E LEFT JOIN (
        SELECT ${f.join(", ")}, min(ID) as ID
        FROM temp
        GROUP BY ${g.join(", ")}
    ) F ON
        ${w.join(" AND \n\t")}
    WHERE F.ID IS NULL
)`
            cmd.push(c)
            console.log(c)
        }
        fields[6]="Line"
    }
    return cmd
}



const db = connect()
readissues("./issues-20230731.csv")
readscans("./scans-20230731.csv")

//dedupeissues()
//readissues("./Issues Filtered - 2023-07-10 11h56m04s.csv")
//readscans("./Scans Filtered - 2023-07-10 11h55m33s.csv")