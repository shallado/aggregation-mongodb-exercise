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

// ------------- Understanding the $isoWeekYear Operator ---------------------
// persons collection

// work with the same aggregation operation from the previous exercise
// add a group stage
// group by birth date display value in the field birthDate
// have a new field numPersons value should be total people born each year
// add a sort stage
// sort by numPersons field in descending order

db.persons.aggregate([{
    $project: {
      _id: 0,
      name: 1,
      email: 1,
      gender: 1,
      birthDate: {
        $convert: {
          input: '$dob.date',
          to: 'date',
        },
      },
      age: '$dob.age',
      location: {
        type: 'Point',
        coordinates: [{
            $convert: {
              input: '$location.coordinates.longitude',
              to: 'double',
              onError: 0.0,
              onNull: 0.0,
            },
          },
          {
            $convert: {
              input: '$location.coordinates.latitude',
              to: 'double',
              onError: 0.0,
              onNull: 0.0,
            },
          },
        ],
      },
    },
  },
  {
    $project: {
      email: 1,
      location: 1,
      gender: 1,
      birthDate: 1,
      age: 1,
      fullName: {
        $concat: [{
            $toUpper: {
              $substrCP: ['$name.first', 0, 1],
            },
          },
          {
            $substrCP: ['$name.first', 1, '$name.first'.length],
          },
          ' ',
          {
            $toUpper: {
              $substrCP: ['$name.last', 0, 1],
            },
          },
          {
            $substrCP: ['$name.last', 1, '$name.last'.length],
          },
        ],
      },
    },
  },
  {
    $group: {
      _id: {
        birthDate: {
          $isoWeekYear: '$birthDate'
        },
      },
      numPersons: {
        $sum: 1,
      },
    },
  },
]);

// ------------- Pushing Elements Into Newly Created Arrays -------------------
// friends collection

// insert the data in the array-data.json file

// use group stage aggregation
  // group by age field and provide fieldName as age as well
  // add a new field allHobbies will contain an array of hobbies for each age group

db.friends.aggregate([{
  $group: {
    _id: {
      age: '$age'
    },
    allHobbies: {
      $push: '$hobbies'
    }
  }
}]);

// ------------- Understanding the $unwind Stage ---------------------
// friends collection

// take the hobbies field array for each document and give me each value as a separate document
db.friends.aggregate([{
  $unwind: '$hobbies'
}]);

// ------------- Eliminating Duplicate Values ---------------------
// friends collection

// add a group stage aggregation to the previous aggregation located above
  // group by age field and provide fieldName as age as well
  // add a new field allHobbies will contain an array of hobbies for each age group and make sure no duplicates inside the array

db.friends.aggregate([{
  $unwind: '$hobbies'
}, {
  $group: {
    _id: {
      age: '$age'
    },
    allHobbies: {
      $addToSet: '$hobbies'
    }
  }
}]);

// ------------- Using Projection with Arrays ---------------------
// friends collection

// give me document with no id field 
// display examScores field so that it only display the first exam score info
db.friends.aggregate([{
  $project: {
    _id: 0,
    examScores: {
      $slice: ['$examScores', 0, 1]
    }
  }
}]);

// give me a document with no id field
// change examScores field so that it only displays that last 2 exam score
db.friends.aggregate([{
  $project: {
    _id: 0,
    examScores: {
      $slice: ['$examScores', 1, 2]
    }
  }
}]);

// ------------- Getting the Length of Array ---------------------
// friends collection

// give me a documents with no id field
// display a field with the name numScores with a value of the array length examScores
db.friends.aggregate([{
  $project: {
    _id: 0,
    numScores: {
      $size: '$examScores'
    }
  }
}]);

// ------------- Using the $filter Operator ---------------------
// friends collection

// give document with no id field
// display examScores field with scores higher than 60
db.friends.aggregate([{
  $project: {
    _id: 0,
    examScores: {
      $filter: {
        input: '$examScores',
        as: 'examInfo',
        cond: {
          $gt: ['$$examInfo.score', 60]
        }
      }
    }
  }
}]);

// ------------- Applying Multiple Operations to our Array ---------------------
// friends collection

// take the examScores field array for each document and give me each value as a separate document
// display only these fields _id, name, age, score value is the score from the examScores field 
// sort with the field score in descending order
// group by id, display a field named name with a value of the first document in the group with name field value, display a field with maxScore with a value of the highest score
// sort by maxScore field in descending order

db.friends.aggregate([{
  $unwind: '$examScores'
}, {
  $project: {
    name: 1,
    age: 1,
    score: '$examScores.score'
  }
}, {
  $sort: {
    score: -1
  }
}, {
  $group: {
    _id: '$_id',
    name: {
      $first: '$name'
    },
    maxScore: {
      $max: '$score'
    }
  }
}, {
  $sort: {
    maxScore: -1
  }
}]);

// ------------- Understanding $bucket ---------------------
// persons collection

// give me an output where it shows the distribution of people in age groups of 18, 30, 40, 50, 60, 120
// fields are 
  // _id with value is the distribution numbers above 
  // numPersons value total people in the age group
  // averageAge value avg age of people in that age group

db.persons.aggregate([{
  $bucket: {
    groupBy: '$dob.age',
    boundaries: [18, 30, 40, 50, 60, 120],
    output: {
      numPersons: {
        $sum: 1
      },
      averageAge: {
        $avg: '$dob.age'
      }
    }
  }
}]);

// perform that same aggregation but a different way
db.persons.aggregate([{
  $bucketAuto: {
    groupBy: '$dob.age',
    buckets: 5,
    output: {
      numPersons: {
        $sum: 1
      },
      averageAge: {
        $avg: '$dob.age'
      }
    }
  }
}]);

// ------------- Diving Into Additional Stages ---------------------
// persons collection

// give me people with gender being male
// do not display _id field
// display name field with value of full name, birthDate with a value of date,
// sort by birthDate in ascending order
// limit 10 results
db.persons.aggregate([{
  $match: {
    gender: 'male'
  }
}, {
  $project: {
    _id: 0,
    name: {
      $concat: ['$name.first', ' ', '$name.last']
    },
    birthDate: {
      $toDate: '$dob.date'
    }
  }
}, {
  $sort: {
    birthDate: 1
  }
}, {
  $limit: 10
}]);

// repeat the same aggregation but skip the first 10 documents
db.persons.aggregate([{
  $match: {
    gender: 'male'
  }
}, {
  $project: {
    _id: 0,
    name: {
      $concat: ['$name.first', ' ', '$name.last']
    },
    birthDate: {
      $toDate: '$dob.date'
    }
  }
}, {
  $sort: {
    birthDate: 1
  }
}, {
  $skip: 10
}, {
  $limit: 10
}]);

// ------------- Writing Pipeline Results Into a New Collection ----------------
// persons collection

// do not display the field id
// display the field email, birthDate with a value of dob date converted to date, age with a value of age from dob field, geoJSON object with long and lat, fullName with value of first and last name with first letters being capitalized
// this aggregations saved to a new collection named transformedPersons
db.persons.aggregate([{
  $project: {
    _id: 0,
    email: 1,
    birthDate: {
      $toDate: '$dob.date'
    },
    age: '$dob.age',
    location: {
      type: 'Point',
      coordinates: [{
        $toDouble: '$location.coordinates.longitude'
      }, {
        $toDouble: '$location.coordinates.latitude'
      }]
    },
    fullName: {
      $concat: [{
        $toUpper: {
          $substrCP: ['$name.first', 0, 1]
        }
      }, {
        $substrCP: ['$name.first', 1, { $strLenCP: '$name.first' }]
      }, 
      ' ', {
        $toUpper: {
          $substrCP: ['$name.last', 0, 1]
        }
      }, {
        $substrCP: ['$name.last', 1, { $strLenCP: '$name.last' }]
      }]
    }
  }
}, {
  $out: 'transformedPersons'
}]);

// ------------- Working with the $geoNear Stage -------------------
// transformedPersons collection

// index the location field to be able to use geoJSON objects

// give me people that are near coordinates -18.4, -42.8
  // that farthest a person can be from the coordinates is 1000000
  // give me only people who's age is greater than 30
// limit 10

db.transformedPersons.aggregate([{
  $geoNear: {
    near: {
      type: 'Point',
      coordinates: [-18.4, -42.8]
    },
    distanceField: 'distance',
    maxDistance: 1000000,
    query: {
      age: {
        $gt: 30
      }
    }
  }
}, {
  $limit: 10
}]);