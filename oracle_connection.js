const oracledb = require("oracledb");


exports.POLL_ORACLE_DEV = async (query, data) => {
    let connection;
    let Poolconnection;
    try {
        Poolconnection = await oracledb.createPool({
            user: process.env.dev_user,
            password: process.env.dev_password,
            connectString: process.env.dev_connectString,
            poolMin: 1,
            poolMax: 10,
            poolIncrement: 1,
        });
        connection = await Poolconnection.getConnection();
        await connection.executeMany(query, data, { autoCommit: true });
        console.log("finished executing");
        
    } catch (error) {
        console.log("error in Pool Connection", error);
    } finally {
        if (connection) {
            try {
                // Rollback if you are using manual transactions (when autoCommit is false)
                if (!connection.autoCommit) {
                    await connection.rollback();
                }
            } catch (err) {
                console.error("Error during rollback", err);
            }

            // Always release the connection back to the pool
            try {
                await connection.release();
            } catch (err) {
                console.error("Error releasing connection", err);
            }
        }
    }
};

exports.POLL_ORACLE_PROD = async (query, data) => {
    let connection;
    let Poolconnection;
    try {
        Poolconnection = await oracledb.createPool({
            user: process.env.prod_user,
            password: process.env.prod_password,
            connectString: process.env.prod_connectString,
            poolMin: 1,
            poolMax: 10,
            poolIncrement: 1,
        });
        connection = await Poolconnection.getConnection();
        await connection.executeMany(query, data, { autoCommit: true });
    } catch (error) {
        console.log("error in Pool Connection", error);
    } finally {
        connection.rollback();
        connection.release();
    }
};



exports.NORMAL_ORACLE_DEV = async (query) => {
    let connection;
    try {
        connection = await oracledb.getConnection({
            user: process.env.dev_user,
            password: process.env.dev_password,
            connectString: process.env.dev_connectString,
        });
        const result = await connection.execute(query, [], {
            outFormat: oracledb.OUT_FORMAT_OBJECT,
        });
        connection.commit();

        return result;
    } catch (err) {
        console.error(err);
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch (err) {
                console.error(err);
            }
        }
    }
};

exports.NORMAL_ORACLE_PROD = async (query) => {
    let connection;
    try {
        connection = await oracledb.getConnection({
            user: process.env.prod_user,
            password: process.env.prod_password,
            connectString: process.env.prod_connectString,
        });
        const result = await connection.execute(query, [], {
            outFormat: oracledb.OUT_FORMAT_OBJECT,
        });
        connection.commit();

        return result;
    } catch (err) {
        console.error(err);
    } finally {
        if (connection) {
            try {
                await connection.close();
            } catch (err) {
                console.error(err);
            }
        }
    }
};