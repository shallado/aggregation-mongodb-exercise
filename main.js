// mongoimport persons.json - d test - db - c persons--jsonArray--drop

// ------------- Using the Aggregation Framework ---------------------
// persons collection

// using aggregation framework to find documents that have a gender of female
db.persons.aggregate([{
  $match: {
    gender: 'female'
  }
}]).pretty();