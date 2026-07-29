// Local single-node MongoDB replica set for Debezium Change Streams.
// Authentication is intentionally disabled in this local demo. Production
// deployments must enable authentication/TLS and use a dedicated read user.

try {
  rs.status();
} catch (e) {
  if (e.codeName === "NotYetInitialized" || e.code === 94) {
    rs.initiate({
      _id: "rs0",
      members: [{ _id: 0, host: "mongodb:27017" }]
    });
  } else {
    throw e;
  }
}

// Wait until the single member becomes writable before seeding data.
let attempts = 60;
while (attempts-- > 0) {
  try {
    if (db.getSiblingDB("admin").runCommand({ hello: 1 }).isWritablePrimary) {
      break;
    }
  } catch (e) {
    // Election is still in progress.
  }
  sleep(1000);
}
if (attempts <= 0) {
  throw new Error("MongoDB replica set did not elect a primary");
}

db = db.getSiblingDB("mydb_mongo");
const products = [
  {
    sku: "PROD-001",
    name: "Gaming Mouse",
    category: "Electronics",
    price: 49.99,
    stock: 150,
    status: "active",
    created_at: new Date()
  },
  {
    sku: "PROD-002",
    name: "Mechanical Keyboard",
    category: "Electronics",
    price: 129.99,
    stock: 80,
    status: "active",
    created_at: new Date()
  },
  {
    sku: "PROD-003",
    name: "USB-C Hub",
    category: "Accessories",
    price: 39.99,
    stock: 200,
    status: "active",
    created_at: new Date()
  }
];

for (const product of products) {
  db.products.updateOne(
    { sku: product.sku },
    { $setOnInsert: product },
    { upsert: true }
  );
}

print("MongoDB initialized: replica set rs0 and mydb_mongo.products");