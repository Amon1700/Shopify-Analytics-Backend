const express = require("express");
const cors = require("cors");
const analyticsRoute = require("./src/analytics/analyticsRoute");
const { connect } = require("./src/db/connect");
require("dotenv").config();

const app = express();

app.use(
  cors({
    origin: process.env.CORSORIGIN,
    credentials: true,
  })
);

app.use("/analytics", analyticsRoute);

connect()
  .then(() => {
    app.listen(process.env.PORT || 3000, () => {
      console.log(`server running on port ${process.env.PORT}`);
    });
  })
  .catch((err) => {
    console.log("connection error : ", err);
  });
