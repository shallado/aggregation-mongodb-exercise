// mongoimport persons.json - d test - db - c persons--jsonArray--drop

// ------------- Using the Aggregation Framework ---------------------
// persons collection

// using aggregation framework to find documents that have a gender of female
db.persons.aggregate([{
  $match: {
    gender: 'female'
  }
}]).pretty();

// ------------- Understanding the Group Stage ---------------------
// persons collection

// add to the previous query and group the documents by state
// give me the total of people for each state 

db.persons.aggregate([{
  $match: {
    gender: 'female'
  },
}, {
  $group: {
    _id: {
      state: '$location.state'
    },
    totalPersons: {
      $sum: 1
    }
  }
}]).pretty();

// check to see if indeed a state has the many people associated with it
db.persons.find({
  'location.state': 'bursa'
}).pretty();

// ------------- Diving Deeper Into the Group Stage ---------------------
// persons collection

// build off the previous aggregation and add the sort stage to the aggregation pipeline 
// sort by totalPersons field in descending order
db.persons.aggregate([{
  $match: {
    gender: 'female'
  }
}, {
  $group: {
    _id: {
      state: '$location.state'
    },
    totalPersons: {
      $sum: 1
    }
  }
}, {
  $sort: {
    totalPersons: -1
  }
}])

// Assignment: The Aggregation Framework
// build a pipeline where you only find persons older than 50, group them by gender, find out how many persons you have by gender, what the average age is, order the output by total persons in descending order
db.persons.aggregate([{
  $match: {
    'dob.age': {
      $gt: 50
    }
  },
}, {
  $group: {
    _id: {
      gender: '$gender'
    },
    totalGender: {
      $sum: 1
    },
    avgAge: {
      $avg: '$dob.age'
    }
  }
}, {
  $sort: {
    totalGender: -1
  }
}]);