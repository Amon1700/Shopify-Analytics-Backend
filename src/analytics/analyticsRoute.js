const analyticsRoute = require("express").Router();

const analyticsCtrl = require("./analyticsCtrl");

analyticsRoute.get("/data1", analyticsCtrl.data1);
analyticsRoute.get("/data2", analyticsCtrl.data2);
analyticsRoute.get("/data3", analyticsCtrl.data3);
analyticsRoute.get("/data4", analyticsCtrl.data4);
analyticsRoute.get("/data5", analyticsCtrl.data5);

module.exports = analyticsRoute;
