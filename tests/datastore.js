"use strict";
var path = require("path");
var Datastore = require(path.join(".."));
var adapter = {
    "get": function() {},
    "update": function() {},
    "insert": function() {},
    "find": function() {}
};
var Promise = require("bluebird");
var datastore = new Datastore(adapter);
var toRestores = [];
var sinon = require("sinon");
var assert = require("assert");
describe("Datastore", function() {
    afterEach(function() {
        toRestores.forEach(function(stub) {
            stub.restore();
        });
    });

    describe("get", function() {
        var spy;
        beforeEach(function() {
            spy = sinon.spy(adapter, "get");
            toRestores.push(spy);
            datastore.get("type", "id");
        });
        it("call adapter.get with the right type", function() {
            assert.equal(spy.getCall(0)
                .args[0], "type");
        });

        it("call adapter.get with the right id", function() {
            assert.equal(spy.getCall(0)
                .args[1], "id");
        });
    });

    describe("update", function() {
        var spy;
        beforeEach(function() {
            spy = sinon.spy(adapter, "update");
            toRestores.push(spy);
            datastore.update("type", "id", {
                "field": "value"
            });
        });
        it("call adapter.update with the right type", function() {
            assert.equal(spy.getCall(0)
                .args[0], "type");
        });

        it("call adapter.update with the right id", function() {
            assert.equal(spy.getCall(0)
                .args[1], "id");
        });

        it("call adapter.update with the right data", function() {
            assert.deepEqual(spy.getCall(0)
                .args[2], {
                    "field": "value"
                });
        });
    });

    describe("insert", function() {
        var spy;
        beforeEach(function() {
            spy = sinon.spy(adapter, "insert");
            toRestores.push(spy);
            datastore.insert("type", {
                "field": "value"
            });
        });
        it("call adapter.insert with the right type", function() {
            assert.equal(spy.getCall(0)
                .args[0], "type");
        });

        it("call adapter.insert with the right data", function() {
            assert.deepEqual(spy.getCall(0)
                .args[1], {
                    "field": "value"
                });
        });
    });

    describe("find", function() {
        var spy;
        beforeEach(function() {
            spy = sinon.spy(adapter, "find");
            toRestores.push(spy);
            datastore.find("type", {
                "option": "value"
            });
        });
        it("call adapter.find with the right type", function() {
            assert.equal(spy.getCall(0)
                .args[0], "type");
        });

        it("call adapter.find with the right options", function() {
            assert.deepEqual(spy.getCall(0)
                .args[1], {
                    "option": "value"
                });
        });
    });

    describe("getFindStream", function() {
        it("emit end when no data are available", function(done) {
            var stub = sinon.stub(datastore, "find").callsFake(
                function() {
                    return new Promise(function(
                        resolve) {
                        resolve([]);
                    });
                });
            toRestores.push(stub);
            datastore
                .getFindStream("type", {
                    "option": "value"
                })
                .on("end", function() {
                    done();
                })
                .on("data", function() {});
        });

        it("emit data when data are available", function(done) {
            var stub = sinon.stub(datastore, "find").callsFake(
                function(type, options) {
                    var results = [];
                    if (options.offset === 0) {
                        results = [{
                            "field": "value"
                        }];
                    }

                    return new Promise(function(
                        resolve) {
                        resolve(results);
                    });
                });
            toRestores.push(stub);
            datastore
                .getFindStream("type", {
                    "option": "value"
                })
                .on("data", function(data) {
                    assert.deepEqual(data, {
                        "field": "value"
                    });
                    done();
                });
        });

        it("respect optional limit", function(done) {
            var stub = sinon.stub(datastore, "find").callsFake(
                function(type, options) {
                    var results = [];
                    while (results.length < options
                        .limit) {
                        results.push({
                            "field": "value"
                        });
                    }
                    return new Promise(function(
                        resolve) {
                        resolve(results);
                    });
                });
            var results = [];
            toRestores.push(stub);
            datastore
                .getFindStream("type", {
                    "limit": 17
                })
                .on("data", function(data) {
                    results.push(data);
                })
                .on("end", function() {
                    assert.equal(results.length, 17);
                    done();
                });
        });

        it("emit error", function(done) {
            var stub = sinon.stub(datastore, "find").callsFake(
                function(type, options) {
                    if (options.offset > 0) {
                        return new Promise(function(
                            resolve) {
                            resolve([]);
                        });
                    }
                    return new Promise(function(
                        resolve, reject) {
                        reject("error");
                    });
                });
            toRestores.push(stub);
            datastore.getFindStream("type")
                .on("data", function() {

                })
                .on("error", function() {
                    done();
                });
        });

    });


});
