module net.pincette.jes.util {
  requires java.json;
  requires net.pincette.json;
  requires net.pincette.mongo;
  requires net.pincette.common;
  requires org.mongodb.driver.reactivestreams;
  requires org.mongodb.bson;
  requires org.mongodb.driver.core;
  requires net.pincette.rs;
  requires com.fasterxml.jackson.dataformat.cbor;
  requires kafka.clients;
  requires java.logging;
  requires async.http.client;
  requires kafka.streams;
  requires typesafe.config;
  requires org.reactivestreams;
  exports net.pincette.jes.util;
}
