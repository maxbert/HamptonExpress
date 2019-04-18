var createError = require('http-errors');
var express = require('express');
var path = require('path');
var cookieParser = require('cookie-parser');
var logger = require('morgan');
var userdb = require('./db')
var passport = require('passport');
var Strategy = require('passport-http-bearer').Strategy;

passport.use(new Strategy(
  function(token, cb) {
    userdb.users.findByToken(token, function(err, user) {
      if (err) { return cb(err); }
      if (!user) { return cb(null, false); }
      return cb(null, user);
    });
  }));


var app = express();


// view engine setup
app.set('views', path.join(__dirname, 'views'));
app.set('view engine', 'pug');

app.use(logger('dev'));
app.use(express.json());
app.use(express.urlencoded({ extended: false }));
app.use(cookieParser());
app.use(express.static(path.join(__dirname, 'public')));

var db = require('./queries');

app.get('/readings', passport.authenticate('bearer', { session: false }), db.getReadings);//done
app.post('/readings', passport.authenticate('bearer', { session: false }), db.importReadings);
app.post('/patients',passport.authenticate('bearer', { session: false }), db.importPatients);//done
app.get('/columns',passport.authenticate('bearer', { session: false }), db.getColumns)
app.get('/column', passport.authenticate('bearer', { session: false }),db.getColumnsName)
app.get('/patient', passport.authenticate('bearer', { session: false }),db.getPatient)//done
app.get('/patients', passport.authenticate('bearer', { session: false }),db.getPatients)//done
////////
 app.delete('/patient', passport.authenticate('bearer', {session: false}), db.deletePatient)//done
// app.update('/patients', passport.authenticate('bearer', {session: false}), db.updatePatient)
app.delete('/readings', passport.authenticate('bearer', {session: false}), db.calculateDaysBefore)
// app.update('/readings',  passport.authenticate('bearer', {session: false}), db.updateReadings)
app.post('/login', db.login)

// catch 404 and forward to error handler
app.use(function(req, res, next) {
  next(createError(404));
});

// error handler
app.use(function(err, req, res, next) {
  // set locals, only providing error in development
  res.locals.message = err.message;
  res.locals.error = req.app.get('env') === 'development' ? err : {};

  // render the error page
  res.status(err.status || 500);
  res.render('error');
});

module.exports = app;
