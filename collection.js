const initCassandra = require("./cassandra");

const initCollection = () => {
  const cassandra = initCassandra();


  return (tableName) => {
    const limit = query => (number) => {
      query.limit = number;
      const theRealSort = sort(query);
      const theRealSkip = skip(query);
      const theRealToArray = toArray(query);
      return {
        sort: theRealSort,
        skip: theRealSkip,
        toArray: theRealToArray
      };
    };

    const skip = query => (number) => {
      query.skip = number;
      const theRealSort = sort(query);
      const theRealLimit = limit(query);
      const theRealToArray = toArray(query);
      return {
        sort: theRealSort,
        limit: theRealLimit,
        toArray: theRealToArray
      }
    };

    const sort = query => (sortQuery) => {
   
      query.sort = sortQuery;
      const theRealLimit = limit(query);
      const theRealSkip = skip(query);
      const theRealToArray = toArray(query);
      return {
        limit: theRealLimit,
        skip: theRealSkip,
        toArray: theRealToArray
      }
    }
    const count = query => async () => {
      let cql = `SELECT COUNT(*) FROM test1.${tableName}`;
      let results = await cassandra.execute(cql);
      return parseInt(results.rows[0].count);
    }
    const toArray = query => async () => {
      let selectedProjection;
      let stringWhere;
      if (query.projection !== null) {
        if (Object.keys(query.projection).length) {
          let objects = query.projection;
          let results = [];

          results = Object.keys(objects).filter(e => {
            if (objects[e] === 1) {
              return true;
            } else {
              return false;
            }
          });
          selectedProjection = String(results);
        } else {
          selectedProjection = "*"
        }
      } else {
        selectedProjection = "*";
      }
      let limit = query.limit ? `LIMIT ${query.limit}` : '';
      let cql = `SELECT ${selectedProjection} FROM test1.${tableName} ${limit}`;

      if (query.where !== null && Object.keys(query.where).length) {
        let arr = Object.keys(query.where).map(es => `${es}=${typeof query.where[es] === "number" ? query.where[es] : `'${query.where[es]}'`}`);
        stringWhere = arr.join(" AND ");
        cql = `SELECT ${selectedProjection} FROM test1.${tableName} WHERE ${stringWhere} ${limit} ALLOW FILTERING`;
      }

      let results = await cassandra.execute(cql);
      return results.rows;
    }

    const insertOne = async (data) => {
      try {
        await cassandra.execute(`INSERT INTO test1.${tableName} JSON '${JSON.stringify(data)}'`);
        return "success";
      } catch (err) {
        return err;
      }
    }
    const find = (filter, option) => {
      let query = { where: filter ? filter : null, projection: option ? option.projection : null, skip: null, limit: null, sort: null, };
      const theRealLimit = limit(query);
      const theRealSort = sort(query);
      const theRealSkip = skip(query);
      const theRealToArray = toArray(query);
      const theRealCount = count(query);

      return {
        limit: theRealLimit,
        sort: theRealSort,
        skip: theRealSkip,
        toArray: theRealToArray,
        count: theRealCount
      }
    };
    const findOne = async (filter, option) => {
      let query = { where: null, projection: option ? option.projection : null, skip: null, limit: null, sort: null, };
      let selectedProjection;
      let stringWhere = "";
      if (filter !== null && Object.keys(filter).length) {
        let arr = Object.keys(filter).map(es => `${es}=${typeof filter[es] === "number" ? filter[es] : `'${filter[es]}'`}`);
        stringWhere = arr.join(" AND ");
      }
      if (query.projection !== null) {
        if (Object.keys(query.projection).length) {
          let objects = query.projection;
          let results = [];

          results = Object.keys(objects).filter(e => {
            if (objects[e] === 1) {
              return true;
            } else {
              return false;
            }
          });
          selectedProjection = String(results);
        } else {
          selectedProjection = "*"
        }
      } else {
        selectedProjection = "*";
      }

      let cql = `SELECT * FROM test1.${tableName} WHERE ${stringWhere} LIMIT 1 ALLOW FILTERING`;
      let results = await cassandra.execute(cql);
      return results.rows[0];
    };

    const deleteOne = async (column, value) => {
      "DELETE ${params.keyspace_name}.${params.table_name} FROM ${params.table_name} WHERE ${whereClause.column_name}='${whereClause.value}'"
    };
    return { find, findOne, insertOne }
  }
};

module.exports = initCollection;