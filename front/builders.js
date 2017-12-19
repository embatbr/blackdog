// Builds things (e.g., queries)



var build_query = (body) => {
    console.log(body);

    var fields_expr = body.fields.join(',\n\t');
    var where_expr = '\r';

    query = `SELECT
    ${fields_expr}
FROM
    raichu_flattened.complains
${where_expr}
LIMIT
    10000;
`
    // return {
    //     status: true,
    //     payload: query
    // };
    return query;
}


module.exports = {
    build_query: build_query
};
