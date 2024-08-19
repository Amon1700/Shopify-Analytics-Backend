const analyticsService = require("./analyticsService");

const analyticsCtrl = {
  data1: async (req, res) => {
    try {
      const result = await analyticsService.data1();

      if (result instanceof Error) {
        return res.status(400).send(result.message);
      }

      return res.status(200).send(result);
    } catch (error) {
      return res.status(400).send(error);
    }
  },

  data2: async (req, res) => {
    try {
      const result = await analyticsService.data2();

      if (result instanceof Error) {
        return res.status(400).send(result.message);
      }

      return res.status(200).send(result);
    } catch (error) {
      return res.status(400).send(error);
    }
  },

  data3: async (req, res) => {
    try {
      const result = await analyticsService.data3();

      if (result instanceof Error) {
        return res.status(400).send(result.message);
      }

      return res.status(200).send(result);
    } catch (error) {
      return res.status(400).send(error);
    }
  },

  data4: async (req, res) => {
    try {
      const result = await analyticsService.data4();

      if (result instanceof Error) {
        return res.status(400).send(result.message);
      }

      return res.status(200).send(result);
    } catch (error) {
      return res.status(400).send(error);
    }
  },

  data5: async (req, res) => {
    try {
      const result = await analyticsService.data5();

      if (result instanceof Error) {
        return res.status(400).send(result.message);
      }

      return res.status(200).send(result);
    } catch (error) {
      return res.status(400).send(error);
    }
  },
};

module.exports = analyticsCtrl;
