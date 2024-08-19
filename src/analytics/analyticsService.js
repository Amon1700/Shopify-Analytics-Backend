const { getDb } = require("../db/connect");

const analyticsService = {
  data1: async () => {
    try {
      const db = getDb();

      const pipeline = [
        {
          $facet: {
            daily: [
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $group: {
                  _id: {
                    day: { $dayOfYear: "$created_at" },
                    year: { $year: "$created_at" },
                  },
                  total: {
                    $sum: {
                      $toDouble: "$total_price_set.shop_money.amount",
                    },
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.day": 1,
                },
              },
              {
                $group: {
                  _id: null,
                  salesData: {
                    $push: {
                      year: "$_id.year",
                      day: "$_id.day",
                      total: "$total",
                    },
                  },
                },
              },
              {
                $addFields: {
                  new: {
                    $map: {
                      input: {
                        $range: [1, { $size: "$salesData" }],
                      },
                      as: "index",
                      in: {
                        $multiply: [
                          {
                            $divide: [
                              {
                                $subtract: [
                                  {
                                    $arrayElemAt: [
                                      "$salesData.total",
                                      "$$index",
                                    ],
                                  },
                                  {
                                    $arrayElemAt: [
                                      "$salesData.total",
                                      {
                                        $subtract: ["$$index", 1],
                                      },
                                    ],
                                  },
                                ],
                              },
                              {
                                $arrayElemAt: [
                                  "$salesData.total",
                                  {
                                    $subtract: ["$$index", 1],
                                  },
                                ],
                              },
                            ],
                          },
                          100,
                        ],
                      },
                    },
                  },
                },
              },
              {
                $set: {
                  salesData: {
                    $map: {
                      input: {
                        $range: [0, { $size: "$salesData" }],
                      },
                      as: "index",
                      in: {
                        $mergeObjects: [
                          {
                            $arrayElemAt: ["$salesData", "$$index"],
                          },
                          {
                            $cond: {
                              if: {
                                $gt: ["$$index", 0],
                              },
                              then: {
                                growthRate: {
                                  $arrayElemAt: [
                                    "$new",
                                    {
                                      $subtract: ["$$index", 1],
                                    },
                                  ],
                                },
                              },
                              else: { growthRate: 0 },
                            },
                          },
                        ],
                      },
                    },
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  salesData: 1,
                },
              },
            ],
            monthly: [
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $group: {
                  _id: {
                    year: { $year: "$created_at" },
                    month: { $month: "$created_at" },
                  },
                  total: {
                    $sum: {
                      $toDouble: "$total_price_set.shop_money.amount",
                    },
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.month": 1,
                },
              },
              {
                $group: {
                  _id: null,
                  salesData: {
                    $push: {
                      year: "$_id.year",
                      month: "$_id.month",
                      total: "$total",
                    },
                  },
                },
              },
              {
                $addFields: {
                  new: {
                    $map: {
                      input: {
                        $range: [1, { $size: "$salesData" }],
                      },
                      as: "index",
                      in: {
                        $multiply: [
                          {
                            $divide: [
                              {
                                $subtract: [
                                  {
                                    $arrayElemAt: [
                                      "$salesData.total",
                                      "$$index",
                                    ],
                                  },
                                  {
                                    $arrayElemAt: [
                                      "$salesData.total",
                                      {
                                        $subtract: ["$$index", 1],
                                      },
                                    ],
                                  },
                                ],
                              },
                              {
                                $arrayElemAt: [
                                  "$salesData.total",
                                  {
                                    $subtract: ["$$index", 1],
                                  },
                                ],
                              },
                            ],
                          },
                          100,
                        ],
                      },
                    },
                  },
                },
              },
              {
                $set: {
                  salesData: {
                    $map: {
                      input: {
                        $range: [0, { $size: "$salesData" }],
                      },
                      as: "index",
                      in: {
                        $mergeObjects: [
                          {
                            $arrayElemAt: ["$salesData", "$$index"],
                          },
                          {
                            $cond: {
                              if: {
                                $gt: ["$$index", 0],
                              },
                              then: {
                                growthRate: {
                                  $arrayElemAt: [
                                    "$new",
                                    {
                                      $subtract: ["$$index", 1],
                                    },
                                  ],
                                },
                              },
                              else: { growthRate: 0 },
                            },
                          },
                        ],
                      },
                    },
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  salesData: 1,
                },
              },
            ],
            quaterly: [
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $addFields: {
                  quater: {
                    $ceil: {
                      $divide: [{ $month: "$created_at" }, 3],
                    },
                  },
                  year: { $year: "$created_at" },
                },
              },
              {
                $group: {
                  _id: {
                    year: "$year",
                    quater: "$quater",
                  },
                  total: {
                    $sum: {
                      $toDouble: "$total_price_set.shop_money.amount",
                    },
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.quater": 1,
                },
              },
              {
                $group: {
                  _id: null,
                  salesData: {
                    $push: {
                      year: "$_id.year",
                      quater: "$_id.quater",
                      total: "$total",
                    },
                  },
                },
              },
              {
                $addFields: {
                  new: {
                    $map: {
                      input: {
                        $range: [1, { $size: "$salesData" }],
                      },
                      as: "index",
                      in: {
                        $multiply: [
                          {
                            $divide: [
                              {
                                $subtract: [
                                  {
                                    $arrayElemAt: [
                                      "$salesData.total",
                                      "$$index",
                                    ],
                                  },
                                  {
                                    $arrayElemAt: [
                                      "$salesData.total",
                                      {
                                        $subtract: ["$$index", 1],
                                      },
                                    ],
                                  },
                                ],
                              },
                              {
                                $arrayElemAt: [
                                  "$salesData.total",
                                  {
                                    $subtract: ["$$index", 1],
                                  },
                                ],
                              },
                            ],
                          },
                          100,
                        ],
                      },
                    },
                  },
                },
              },
              {
                $set: {
                  salesData: {
                    $map: {
                      input: {
                        $range: [0, { $size: "$salesData" }],
                      },
                      as: "index",
                      in: {
                        $mergeObjects: [
                          {
                            $arrayElemAt: ["$salesData", "$$index"],
                          },
                          {
                            $cond: {
                              if: {
                                $gt: ["$$index", 0],
                              },
                              then: {
                                growthRate: {
                                  $arrayElemAt: [
                                    "$new",
                                    {
                                      $subtract: ["$$index", 1],
                                    },
                                  ],
                                },
                              },
                              else: { growthRate: 0 },
                            },
                          },
                        ],
                      },
                    },
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  salesData: 1,
                },
              },
            ],
            yearly: [
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $group: {
                  _id: {
                    year: { $year: "$created_at" },
                  },
                  total: {
                    $sum: {
                      $toDouble: "$total_price_set.shop_money.amount",
                    },
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                },
              },
              {
                $group: {
                  _id: null,
                  salesData: {
                    $push: {
                      year: "$_id.year",
                      total: "$total",
                    },
                  },
                },
              },
              {
                $addFields: {
                  new: {
                    $map: {
                      input: {
                        $range: [1, { $size: "$salesData" }],
                      },
                      as: "index",
                      in: {
                        $multiply: [
                          {
                            $divide: [
                              {
                                $subtract: [
                                  {
                                    $arrayElemAt: [
                                      "$salesData.total",
                                      "$$index",
                                    ],
                                  },
                                  {
                                    $arrayElemAt: [
                                      "$salesData.total",
                                      {
                                        $subtract: ["$$index", 1],
                                      },
                                    ],
                                  },
                                ],
                              },
                              {
                                $arrayElemAt: [
                                  "$salesData.total",
                                  {
                                    $subtract: ["$$index", 1],
                                  },
                                ],
                              },
                            ],
                          },
                          100,
                        ],
                      },
                    },
                  },
                },
              },
              {
                $set: {
                  salesData: {
                    $map: {
                      input: {
                        $range: [0, { $size: "$salesData" }],
                      },
                      as: "index",
                      in: {
                        $mergeObjects: [
                          {
                            $arrayElemAt: ["$salesData", "$$index"],
                          },
                          {
                            $cond: {
                              if: {
                                $gt: ["$$index", 0],
                              },
                              then: {
                                growthRate: {
                                  $arrayElemAt: [
                                    "$new",
                                    {
                                      $subtract: ["$$index", 1],
                                    },
                                  ],
                                },
                              },
                              else: { growthRate: 0 },
                            },
                          },
                        ],
                      },
                    },
                  },
                },
              },
              {
                $project: {
                  _id: 0,
                  salesData: 1,
                },
              },
            ],
          },
        },
        {
          $project: {
            daily: { $arrayElemAt: ["$daily.salesData", 0] },
            monthly: { $arrayElemAt: ["$monthly.salesData", 0] },
            quaterly: { $arrayElemAt: ["$quaterly.salesData", 0] },
            yearly: { $arrayElemAt: ["$yearly.salesData", 0] },
          },
        },
      ];

      const result = await db
        .collection("shopifyOrders")
        .aggregate(pipeline)
        .toArray();

      return result;
    } catch (error) {
      return error;
    }
  },

  data2: async () => {
    try {
      const db = getDb();

      const pipeline = [
        {
          $facet: {
            daily: [
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $group: {
                  _id: {
                    day: { $dayOfYear: "$created_at" },
                    year: { $year: "$created_at" },
                  },
                  total: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.day": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  day: "$_id.day",
                  customersAdded: "$total",
                },
              },
            ],
            monthly: [
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $group: {
                  _id: {
                    month: { $month: "$created_at" },
                    year: { $year: "$created_at" },
                  },
                  total: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.month": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  month: "$_id.month",
                  customersAdded: "$total",
                },
              },
            ],
            quaterly: [
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $addFields: {
                  quater: {
                    $ceil: {
                      $divide: [{ $month: "$created_at" }, 3],
                    },
                  },
                  year: { $year: "$created_at" },
                },
              },
              {
                $group: {
                  _id: {
                    year: "$year",
                    quater: "$quater",
                  },
                  total: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.quater": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  quater: "$_id.quater",
                  customersAdded: "$total",
                },
              },
            ],
            yearly: [
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $group: {
                  _id: {
                    year: { $year: "$created_at" },
                  },
                  total: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  customersAdded: "$total",
                },
              },
            ],
          },
        },
      ];

      const result = await db
        .collection("shopifyCustomers")
        .aggregate(pipeline)
        .toArray();

      return result;
    } catch (error) {
      return error;
    }
  },

  data3: async () => {
    try {
      const db = getDb();

      const pipeline = [
        {
          $facet: {
            daily: [
              {
                $lookup: {
                  from: "shopifyOrders",
                  localField: "_id",
                  foreignField: "customer.id",
                  as: "result",
                },
              },
              {
                $match: {
                  $expr: {
                    $gt: [{ $size: "$result" }, 1],
                  },
                },
              },
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $group: {
                  _id: {
                    day: { $dayOfYear: "$created_at" },
                    year: { $year: "$created_at" },
                  },
                  total: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.day": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  day: "$_id.day",
                  repeatCustomers: "$total",
                },
              },
            ],
            monthly: [
              {
                $lookup: {
                  from: "shopifyOrders",
                  localField: "_id",
                  foreignField: "customer.id",
                  as: "result",
                },
              },
              {
                $match: {
                  $expr: {
                    $gt: [{ $size: "$result" }, 1],
                  },
                },
              },
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $group: {
                  _id: {
                    month: { $month: "$created_at" },
                    year: { $year: "$created_at" },
                  },
                  total: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.month": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  month: "$_id.month",
                  repeatCustomers: "$total",
                },
              },
            ],
            quaterly: [
              {
                $lookup: {
                  from: "shopifyOrders",
                  localField: "_id",
                  foreignField: "customer.id",
                  as: "result",
                },
              },
              {
                $match: {
                  $expr: {
                    $gt: [{ $size: "$result" }, 1],
                  },
                },
              },
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $addFields: {
                  quater: {
                    $ceil: {
                      $divide: [{ $month: "$created_at" }, 3],
                    },
                  },
                  year: { $year: "$created_at" },
                },
              },
              {
                $group: {
                  _id: {
                    year: "$year",
                    quater: "$quater",
                  },
                  total: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.quater": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  quater: "$_id.quater",
                  repeatCustomers: "$total",
                },
              },
            ],
            yearly: [
              {
                $lookup: {
                  from: "shopifyOrders",
                  localField: "_id",
                  foreignField: "customer.id",
                  as: "result",
                },
              },
              {
                $match: {
                  $expr: {
                    $gt: [{ $size: "$result" }, 1],
                  },
                },
              },
              {
                $addFields: {
                  created_at: {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $group: {
                  _id: {
                    year: { $year: "$created_at" },
                  },
                  total: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  repeatCustomers: "$total",
                },
              },
            ],
          },
        },
      ];

      const result = await db
        .collection("shopifyCustomers")
        .aggregate(pipeline)
        .toArray();

      return result;
    } catch (error) {
      return error;
    }
  },

  data4: async () => {
    try {
      const db = getDb();

      const pipeline = [
        {
          $group: {
            _id: "$default_address.city",
            total: {
              $sum: 1,
            },
          },
        },
        {
          $project: {
            _id: 0,
            city: "$_id",
            customers: "$total",
          },
        },
      ];

      const result = await db
        .collection("shopifyCustomers")
        .aggregate(pipeline)
        .toArray();

      return result;
    } catch (error) {
      return error;
    }
  },

  data5: async () => {
    try {
      const db = getDb();

      const pipeline = [
        {
          $facet: {
            monthly: [
              {
                $lookup: {
                  from: "shopifyOrders",
                  localField: "_id",
                  foreignField: "customer.id",
                  as: "result",
                },
              },
              {
                $match: {
                  $expr: {
                    $gt: [{ $size: "$result" }, 0],
                  },
                },
              },
              {
                $addFields: {
                  "result.created_at": {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $set: {
                  result: {
                    $sortArray: {
                      input: "$result",
                      sortBy: { created_at: 1 },
                    },
                  },
                },
              },
              {
                $set: {
                  purchase: {
                    $arrayElemAt: ["$result", 0],
                  },
                },
              },
              {
                $group: {
                  _id: {
                    month: {
                      $month: "$purchase.created_at",
                    },
                    year: {
                      $year: "$purchase.created_at",
                    },
                  },
                  total_clv: {
                    $sum: {
                      $toDouble: "$purchase.total_price_set.shop_money.amount",
                    },
                  },
                  customer_count: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.month": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  month: "$_id.month",
                  average_clv: {
                    $divide: ["$total_clv", "$customer_count"],
                  },
                },
              },
            ],
            quaterly: [
              {
                $lookup: {
                  from: "shopifyOrders",
                  localField: "_id",
                  foreignField: "customer.id",
                  as: "result",
                },
              },
              {
                $match: {
                  $expr: {
                    $gt: [{ $size: "$result" }, 0],
                  },
                },
              },
              {
                $addFields: {
                  "result.created_at": {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $set: {
                  result: {
                    $sortArray: {
                      input: "$result",
                      sortBy: { created_at: 1 },
                    },
                  },
                },
              },
              {
                $set: {
                  purchase: {
                    $arrayElemAt: ["$result", 0],
                  },
                },
              },
              {
                $addFields: {
                  quater: {
                    $ceil: {
                      $divide: [
                        {
                          $month: "$purchase.created_at",
                        },
                        3,
                      ],
                    },
                  },
                  year: {
                    $year: "$purchase.created_at",
                  },
                },
              },
              {
                $group: {
                  _id: {
                    year: "$year",
                    quater: "$quater",
                  },
                  total_clv: {
                    $sum: {
                      $toDouble: "$purchase.total_price_set.shop_money.amount",
                    },
                  },
                  customer_count: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                  "_id.quater": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  quater: "$_id.quater",
                  average_clv: {
                    $divide: ["$total_clv", "$customer_count"],
                  },
                },
              },
            ],
            yearly: [
              {
                $lookup: {
                  from: "shopifyOrders",
                  localField: "_id",
                  foreignField: "customer.id",
                  as: "result",
                },
              },
              {
                $match: {
                  $expr: {
                    $gt: [{ $size: "$result" }, 0],
                  },
                },
              },
              {
                $addFields: {
                  "result.created_at": {
                    $dateFromString: {
                      dateString: "$created_at",
                    },
                  },
                },
              },
              {
                $set: {
                  result: {
                    $sortArray: {
                      input: "$result",
                      sortBy: { created_at: 1 },
                    },
                  },
                },
              },
              {
                $set: {
                  purchase: {
                    $arrayElemAt: ["$result", 0],
                  },
                },
              },
              {
                $addFields: {
                  quater: {
                    $ceil: {
                      $divide: [
                        {
                          $month: "$purchase.created_at",
                        },
                        3,
                      ],
                    },
                  },
                  year: {
                    $year: "$purchase.created_at",
                  },
                },
              },
              {
                $group: {
                  _id: {
                    year: {
                      $year: "$purchase.created_at",
                    },
                  },
                  total_clv: {
                    $sum: {
                      $toDouble: "$purchase.total_price_set.shop_money.amount",
                    },
                  },
                  customer_count: {
                    $sum: 1,
                  },
                },
              },
              {
                $sort: {
                  "_id.year": 1,
                },
              },
              {
                $project: {
                  _id: 0,
                  year: "$_id.year",
                  average_clv: {
                    $divide: ["$total_clv", "$customer_count"],
                  },
                },
              },
            ],
          },
        },
      ];

      const result = await db
        .collection("shopifyCustomers")
        .aggregate(pipeline)
        .toArray();

      return result;
    } catch (error) {
      return error;
    }
  },
};

module.exports = analyticsService;
