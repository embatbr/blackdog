var express = require('express');
var logger = require('morgan');
var app = express();
var jade = require('jade');
var body_parser = require('body-parser');

var templates = {
    home: jade.compileFile(__dirname + '/source/templates/home.jade'),
    query: jade.compileFile(__dirname + '/source/templates/query.jade')
};


var config = require('./config')
var builders = require('./builders')


app.use(logger(config.env_name));
app.use(express.static(__dirname + '/static'));
app.use(body_parser.json());
app.use(body_parser.urlencoded({ extended: true }));


app.get('/', (req, resp, next) => {
    try {
        var html = templates['home']({
            title: 'Home'
        })

        resp.send(html)
    }
    catch (e) {
        next(e)
    }
});

app.get('/query', (req, resp, next) => {
    try {
        var html = templates['query']({
            title: 'Consulta',
            results: ''
        })

        resp.send(html)
    }
    catch (e) {
        next(e)
    }
});

app.post('/query', (req, resp) => {
    query = builders.build_query(req.body);

    try {
        var html = templates['query']({
            title: 'Consulta',
            results: query
        })

        resp.send(html)
    }
    catch (e) {
        next(e)
    }
});

app.listen(config.listening_port, () => {
    console.log('Listening on http://localhost:' + config.listening_port)
});