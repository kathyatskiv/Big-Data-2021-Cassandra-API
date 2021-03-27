const express = require("express");
const app = express();

const cassandra = require('cassandra-driver');
const client = new cassandra.Client({
  contactPoints: ['35.238.19.186'],
  localDataCenter: 'us-central1',
  keyspace: 'books_reviews',
  credentials: { username: 'cassandra', password: 'oX44M3SVVZf9' }
});

app.get("/", function(request, response, next){
    
    client.execute('SELECT * FROM all_reviews_by_product_id LIMIT 3;', 
      function(err, results) {
        if (err) console.log(err);
        else response.send(results);
      }
    );
});

//Return all reviews for specified `product_id`
//Return all reviews for specified `product_id` with given `star_rating`
app.route('/reviews/products/:product_id')
  .get(function(request, response, next) {
    let id = request.params["product_id"];
    let rate = request.query.star_rating;

    if(rate == undefined) {

        client.execute(`SELECT review_headline, review_body FROM all_reviews_by_product_id WHERE product_id='${id}'`, 
        function(err, results) {
            if(err) {
                console.log("ERROR\n");
                console.log(err);
            }   
            else response.send(results)
        });

    } else{
        client.execute(`SELECT review_headline, review_body, star_rating FROM all_reviews_by_product_id WHERE product_id='${id}' AND star_rating=${rate} ALLOW FILTERING`, 
        function(err, results) {
            if(err) {
                console.log("ERROR\n");
                console.log(err);
            }         
            else response.send(results)
    });
    }
  });


//Return all reviews for specified `customer_id`
app.route('/reviews/customers/:customer_id')
  .get(function(request, response, next) {
    let id = request.params["customer_id"];
    pool.query(`SELECT review_headline, review_body FROM all_reviews_by_customer_id  WHERE customer_id=${id}`, 
    function(err, results) {
        if(err) {
            console.log("ERROR\n");
            console.log(err);
        }   
        response.send(results)
    });
  });

//Return N most reviewed items (by # of reviews) for a given period of time
app.route('/reviews/popular')
  .get(function(request,response,next) {
    let n = request.query.n;
    let date = request.query.date;

    pool.query(`SELECT product_id, product_title, reviews_amount FROM top_N_products_by_date WHERE review_date = ${date} LIMIT ${ n == undefined ? 1 : n};`, 
    function(err, results) {
      if(err) {
          console.log("ERROR\n");
          console.log(err);
      }   
      response.send(results)
  });
  })

//Return N most productive customers (by # of reviews written for verified purchases) for a given period
app.route('/customers/productive')
.get(function(request,response,next) {
  let n = request.query.n;
  let date = request.query.date;

  pool.query(`SELECT customer_id, reviews_amount FROM top_N_customers_by_date WHERE review_date = ${date} LIMIT ${ n == undefined ? 1 : n};`, 
  function(err, results) {
    if(err) {
        console.log("ERROR\n");
        console.log(err);
    }   
    response.send(results)
});
})

//Return N most productive “haters” (by # of 1- or 2-star reviews) for a given period
app.route('/haters')
  .get(function(request,response,next) {
    let n = request.query.n;

    pool.query(`SELECT customer_id, bad_reviews_amount FROM top_N_haters_by_date WHERE review_date = ${date} LIMIT ${ n == undefined ? 1 : n};`, 
    function(err, results) {
      if(err) {
          console.log("ERROR\n");
          console.log(err);
      }   
      response.send(results)
  });
  })

//Return N most productive “backers” (by # of 4- or 5-star reviews) for a given period
app.route('/backers')
  .get(function(request,response,next) {
    let n = request.query.n;

    pool.query(`SELECT customer_id, good_reviews_amount FROM top_N_backers_by_date WHERE review_date = ${date} LIMIT ${ n == undefined ? 1 : n};`, function(err, results) {
      if(err) {
          console.log("ERROR\n");
          console.log(err);
      }   
      response.send(results)
  });
  })

app.set('port', process.env.PORT || 9042);
app.listen(9042);