/**
 * This can be used in the MongoDB Compass Shell to remove any duplicates fromt the transfers collections
 * keeps the most recenet one
 */
db.click_transfers
  .aggregate([
    {
      $group: {
        _id: '$signature',
        count: { $sum: 1 },
        docs: {
          $push: {
            id: '$_id',
            timestamp: '$timestamp',
          },
        },
      },
    },
    {
      $match: {
        count: { $gt: 1 },
      },
    },
  ])
  .forEach(function (doc) {
    // Sort by timestamp, keep the latest
    doc.docs.sort((a, b) => b.timestamp - a.timestamp);

    // Delete all but the first (most recent)
    for (let i = 1; i < doc.docs.length; i++) {
      db.click_transfers.deleteOne({ _id: doc.docs[i].id });
      print('Deleted duplicate: ' + doc.docs[i].id);
    }
  });

// Then create the unique index
db.click_transfers.createIndex({ signature: 1 }, { unique: true });

/**
 * This can be used in the MongoDB Compass Shell to remove any duplicates from the rewards wallet collection
 */
db.rewards_wallets
  .aggregate([
    {
      $group: {
        _id: {
          wallet_address: '$wallet_address',
          distributor: '$distributor',
          signature: '$signature',
          slot: '$slot',
          timestamp: '$timestamp',
          token: '$token',
          amount: '$amount',
        },
        count: { $sum: 1 },
        docs: {
          $push: {
            id: '$_id',
            timestamp: '$timestamp',
          },
        },
      },
    },
    {
      $match: {
        count: { $gt: 1 },
      },
    },
  ])
  .forEach(function (doc) {
    // Sort by timestamp, keep the latest (or you could keep the first inserted by using _id)
    doc.docs.sort((a, b) => b.timestamp - a.timestamp);

    // Delete all but the first (most recent)
    for (let i = 1; i < doc.docs.length; i++) {
      db.rewards_wallets.deleteOne({ _id: doc.docs[i].id });
      print('Deleted duplicate: ' + doc.docs[i].id);
    }
  });
