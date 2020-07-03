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

// ------------- Working with $project ---------------------
// persons collection

// don't include id
// include gender
// create a new field fullName containing first and last name
db.persons.aggregate([{
  $project: {
    _id: 0,
    gender: 1,
    fullName: {
      $concat: ['$name.first', ' ', '$name.last']
    }
  }
}, {
  $limit: 5
}]);

// repeat the same aggregation above but have the field fullName contain first and last name all uppercase
db.persons.aggregate([{
  $project: {
    _id: 0,
    gender: 1,
    fullName: {
      $concat: [{
          $toUpper: '$name.first'
        },
        ' ', {
          $toUpper: '$name.last'
        }
      ]
    }
  }
}]);

// repeat the same aggregation above but have the field fullName containing first and last name with only first letters being capitalized for each
// try to implementing two ways to capitalize first letter of first and last name
// one just using aggregation and mongo operators 
// one with using one javascript string method
db.persons.aggregate([{
  $project: {
    _id: 0,
    gender: 1,
    fullName: {
      $concat: [{
          $toUpper: {
            $substrCP: [
              '$name.first',
              0,
              1
            ]
          }
        }, {
          $substrCP: [
            '$name.first',
            1,
            '$name.first'.length
          ]
        },
        ' ', {
          $toUpper: {
            $substrCP: [
              '$name.last',
              0,
              1
            ]
          }
        }, {
          $substrCP: [
            '$name.last',
            1,
            '$name.last'.length
          ]
        }
      ]
    }
  }
}]);

// ------------- Turning the Location Into a geoJSON object -------------------
// persons collection

// previous aggregation pipeline should be the same but with fields gender, email, location
// add another project stage to the previous aggregation pipeline above
// make sure this project stage is first in the aggregation pipeline
// exclude field _id
// include name, email, change location field value to geoJSON object remember its longitude, latitude, gender
// longitude, latitude is a string so make sure to convert it to a number
// make sure to have default value just incase convert doesn't work

db.persons.aggregate([{
  $project: {
    _id: 0,
    name: 1,
    email: 1,
    gender: 1,
    location: {
      type: 'Point',
      coordinates: [{
        $convert: {
          input: '$location.coordinates.longitude',
          to: 'double',
          onError: 0.00,
          onNull: 0.00
        }
      }, {
        $convert: {
          input: '$location.coordinates.latitude',
          to: 'double',
          onError: 0.00,
          onNull: 0.00
        }
      }]
    }
  }
}, {
  $project: {
    email: 1,
    location: 1,
    gender: 1,
    fullName: {
      $concat: [{
          $toUpper: {
            $substrCP: [
              '$name.first',
              0,
              1
            ]
          }
        }, {
          $substrCP: [
            '$name.first',
            1,
            '$name.first'.length
          ]
        },
        ' ', {
          $toUpper: {
            $substrCP: [
              '$name.last',
              0,
              1
            ]
          }
        }, {
          $substrCP: [
            '$name.last',
            1,
            '$name.last'.length
          ]
        }
      ]
    }
  }
}]);

// ------------- Tranforming the Birthdate ---------------------
// persons collection

// work with the same aggregation from the previous exercise
// add these fields to the first $project operator
// add a birthDate field with dob birth converted to datatype of date
// add a field age with value of age
// run the aggregation operation see results
// now modify how you convert the birthDate field value using a shortcut operator

db.persons.aggregate([{
  $project: {
    _id: 0,
    name: 1,
    email: 1,
    gender: 1,
    birthDate: {
      $convert: {
        input: '$dob.date',
        to: 'date'
      }
    },
    age: '$dob.age',
    location: {
      type: 'Point',
      coordinates: [{
        $convert: {
          input: '$location.coordinates.longitude',
          to: 'double',
          onError: 0.00,
          onNull: 0.00
        }
      }, {
        $convert: {
          input: '$location.coordinates.latitude',
          to: 'double',
          onError: 0.00,
          onNull: 0.00
        }
      }]
    }
  }
}, {
  $project: {
    email: 1,
    location: 1,
    gender: 1,
    birthDate: 1,
    age: 1,
    fullName: {
      $concat: [{
          $toUpper: {
            $substrCP: [
              '$name.first',
              0,
              1
            ]
          }
        }, {
          $substrCP: [
            '$name.first',
            1,
            '$name.first'.length
          ]
        },
        ' ', {
          $toUpper: {
            $substrCP: [
              '$name.last',
              0,
              1
            ]
          }
        }, {
          $substrCP: [
            '$name.last',
            1,
            '$name.last'.length
          ]
        }
      ]
    }
  }
}]);
