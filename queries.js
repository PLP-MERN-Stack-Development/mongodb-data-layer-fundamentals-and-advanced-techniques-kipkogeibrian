// queries.js - Complete MongoDB queries for Week 1 Tasks

const { MongoClient } = require('mongodb');

// Connection URI (replace with your MongoDB connection string)
const uri = 'mongodb+srv://plp_user:<db_password>@cluster0.u6x9iou.mongodb.net/?retryWrites=true&w=majority&appName=Cluster0';
const dbName = 'plp_bookstore';
const collectionName = 'books';

async function runQueries() {
  const client = new MongoClient(uri);

  try {
    await client.connect();
    console.log('Connected to MongoDB server\n');
    
    const db = client.db(dbName);
    const collection = db.collection(collectionName);

   
    // TASK 2: Basic CRUD Operations
  

    console.log('=== TASK 2: BASIC CRUD OPERATIONS ===\n');

    // 1. Find all books in a specific genre
    console.log('1. Find all Fantasy books:');
    const fantasyBooks = await collection.find({ genre: "Fantasy" }).toArray();
    console.log(fantasyBooks.map(book => `   - "${book.title}" by ${book.author}`));
    console.log();

    // 2. Find books published after a certain year
    console.log('2. Find books published after 1950:');
    const booksAfter1950 = await collection.find({ published_year: { $gt: 1950 } }).toArray();
    console.log(booksAfter1950.map(book => `   - "${book.title}" (${book.published_year})`));
    console.log();

    // 3. Find books by a specific author
    console.log('3. Find books by George Orwell:');
    const orwellBooks = await collection.find({ author: "George Orwell" }).toArray();
    console.log(orwellBooks.map(book => `   - "${book.title}" (${book.published_year})`));
    console.log();

    // 4. Update the price of a specific book
    console.log('4. Update price of "The Great Gatsby" to $12.99:');
    const updateResult = await collection.updateOne(
      { title: "The Great Gatsby" },
      { $set: { price: 12.99 } }
    );
    console.log(`   Modified ${updateResult.modifiedCount} document(s)`);
    
    // Verify the update
    const updatedBook = await collection.findOne({ title: "The Great Gatsby" });
    console.log(`   New price: $${updatedBook.price}`);
    console.log();

    // 5. Delete a book by its title
    console.log('5. Delete "Moby Dick" from collection:');
    const deleteResult = await collection.deleteOne({ title: "Moby Dick" });
    console.log(`   Deleted ${deleteResult.deletedCount} document(s)`);
    
    // Verify remaining count
    const remainingCount = await collection.countDocuments();
    console.log(`   Total books remaining: ${remainingCount}`);
    console.log();

   
    // TASK 3: Advanced Queries
    

    console.log('=== TASK 3: ADVANCED QUERIES ===\n');

    // 1. Find books that are both in stock and published after 2010
    console.log('1. Books in stock and published after 1950:');
    const inStockRecent = await collection.find({ 
      in_stock: true, 
      published_year: { $gt: 1950 } 
    }).toArray();
    console.log(inStockRecent.map(book => `   - "${book.title}" (${book.published_year})`));
    console.log();

    // 2. Use projection to return only title, author, and price
    console.log('2. Fiction books with projection (title, author, price only):');
    const fictionBooksProjection = await collection.find(
      { genre: "Fiction" },
      { projection: { title: 1, author: 1, price: 1, _id: 0 } }
    ).toArray();
    console.log(fictionBooksProjection);
    console.log();

    // 3. Implement sorting by price (both ascending and descending)
    console.log('3. Books sorted by price (ascending):');
    const booksAscending = await collection.find(
      {},
      { projection: { title: 1, price: 1, _id: 0 } }
    ).sort({ price: 1 }).toArray();
    console.log(booksAscending);

    console.log('   Books sorted by price (descending):');
    const booksDescending = await collection.find(
      {},
      { projection: { title: 1, price: 1, _id: 0 } }
    ).sort({ price: -1 }).toArray();
    console.log(booksDescending);
    console.log();

    // 4. Use limit and skip for pagination (5 books per page)
    console.log('4. Pagination - Page 1 (5 books):');
    const page1 = await collection.find(
      {},
      { projection: { title: 1, author: 1, _id: 0 } }
    ).limit(5).skip(0).toArray();
    console.log(page1);

    console.log('   Pagination - Page 2 (5 books):');
    const page2 = await collection.find(
      {},
      { projection: { title: 1, author: 1, _id: 0 } }
    ).limit(5).skip(5).toArray();
    console.log(page2);
    console.log();

    
    // TASK 4: Aggregation Pipeline
   

    console.log('=== TASK 4: AGGREGATION PIPELINE ===\n');

    // 1. Calculate average price of books by genre
    console.log('1. Average price by genre:');
    const avgPriceByGenre = await collection.aggregate([
      {
        $group: {
          _id: "$genre",
          averagePrice: { $avg: "$price" },
          bookCount: { $sum: 1 }
        }
      },
      {
        $sort: { averagePrice: -1 }
      }
    ]).toArray();
    console.log(avgPriceByGenre);
    console.log();

    // 2. Find author with the most books in the collection
    console.log('2. Authors with most books:');
    const authorsMostBooks = await collection.aggregate([
      {
        $group: {
          _id: "$author",
          bookCount: { $sum: 1 }
        }
      },
      {
        $sort: { bookCount: -1 }
      },
      {
        $limit: 3
      }
    ]).toArray();
    console.log(authorsMostBooks);
    console.log();

    // 3. Group books by publication decade and count them
    console.log('3. Books by publication decade:');
    const booksByDecade = await collection.aggregate([
      {
        $group: {
          _id: {
            $subtract: [
              "$published_year",
              { $mod: ["$published_year", 10] }
            ]
          },
          decade: { $first: { 
            $concat: [
              { $toString: { $subtract: [
                "$published_year",
                { $mod: ["$published_year", 10] }
              ] } },
              "s"
            ]
          }},
          count: { $sum: 1 },
          books: { $push: "$title" }
        }
      },
      {
        $sort: { _id: 1 }
      },
      {
        $project: {
          _id: 0,
          decade: 1,
          count: 1,
          sample_books: { $slice: ["$books", 2] }
        }
      }
    ]).toArray();
    console.log(booksByDecade);
    console.log();

    
    // TASK 5: Indexing
    

    console.log('=== TASK 5: INDEXING ===\n');

    // 1. Create index on the title field
    console.log('1. Creating index on title field...');
    await collection.createIndex({ title: 1 });
    console.log('   Index created on title field');

    // 2. Create compound index on author and published_year
    console.log('2. Creating compound index on author and published_year...');
    await collection.createIndex({ author: 1, published_year: 1 });
    console.log('   Compound index created on author and published_year');

    // 3. Use explain() to demonstrate performance improvement
    console.log('3. Performance analysis with explain():');
    
    console.log('   Query with title index:');
    const explainTitle = await collection.find({ title: "The Hobbit" }).explain("executionStats");
    console.log(`   Execution time: ${explainTitle.executionStats.executionTimeMillis}ms`);
    console.log(`   Documents examined: ${explainTitle.executionStats.totalDocsExamined}`);
    
    console.log('   Query with compound index:');
    const explainCompound = await collection.find({ 
      author: "J.R.R. Tolkien", 
      published_year: { $gt: 1930 } 
    }).explain("executionStats");
    console.log(`   Execution time: ${explainCompound.executionStats.executionTimeMillis}ms`);
    console.log(`   Documents examined: ${explainCompound.executionStats.totalDocsExamined}`);

    // List all indexes
    console.log('\n4. Current indexes:');
    const indexes = await collection.indexes();
    console.log(indexes.map(index => `   - ${JSON.stringify(index.key)}`));

  } catch (err) {
    console.error('Error occurred:', err);
  } finally {
    await client.close();
    console.log('\nConnection closed');
  }
}

// Run all queries
runQueries().catch(console.error);
