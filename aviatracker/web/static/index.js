require([
    "esri/Map",
    "esri/views/MapView",
    "esri/Graphic",
    "esri/widgets/Feature",
    "esri/PopupTemplate",
    "esri/layers/FeatureLayer",
    "esri/geometry/support/webMercatorUtils",
    "esri/core/watchUtils",
    "esri/popup/content/LineChartMediaInfo",
    "esri/popup/content/support/ChartMediaInfoValue"
], function(Map, MapView, Graphic, Feature, PopupTemplate, FeatureLayer, webMercatorUtils,
            watchUtils, LineChartMediaInfo, ChartMediaInfoValue) {
    let buckets = [];
    let graphicObjects = {};
    let extentCoords = {
        xMin: 0,
        xMax: 0,
        yMin: 0,
        yMax: 0
    };
    let addStarts;

    const url = document.getElementById('url');
    const urlAttr= url.getAttribute('url');

    let map = new Map({
        basemap: "dark-gray-vector"
    });

    let view = new MapView({
        container: "viewDiv",
        map: map,
        center: [50, 50], // longitude, latitude
        zoom: 1,
        popup: {
            autoOpenEnabled: false
        },
        highlightOptions: {
            color: [255, 255, 255, 0.3],
            haloColor: [123, 229, 224, 0.6],
            haloOpacity: 1,
            fillOpacity: 0.7
        },
        constraints: {
            minScale: 100001000
        }
    });

    let aircraftMarker = {
        type: "picture-marker",
        url: urlAttr,
        angle: 0
    };

    let airportMarker = {
        type: "simple-marker",
        color: [47, 48, 48, 1],
        outline: {
            width: 0.4,
            color: [37, 38, 38, 1]
        },
        size: 4
    };

    let fields = [
        {
            name: "objectID",
            type: "oid"
        }, {
            name: "icao24",
            type: "string"
        }, {
            name: "callsign",
            type: "string"
        }, {
            name: "origin_country",
            type: "string"
        }, {
            name: "longitude",
            type: "double"
        }, {
            name: "latitude",
            type: "double"
        }, {
            name: "baro_altitude",
            type: "double"
        }, {
            name: "velocity",
            type: "double"
        }, {
            name: "true_track",
            type: "double"
        }
    ];

    let airportFields = [
        {
            name: "objectID",
            type: "oid"
        }, {
            name: "icao",
            type: "string"
        }, {
            name: "name",
            type: "string"
        }, {
            name: "city",
            type: "string"
        }, {
            name: "country",
            type: "string"
        }, {
            name: "longitude",
            type: "double"
        }, {
            name: "latitude",
            type: "double"
        }
    ];

    let pathFields = [
        {
            name: "objectID",
            type: "oid"
        }, {
            name: "icao24",
            type: "string"
        }, {
            name: "callsign",
            type: "string"
        }
    ];

    let rotationRenderer = {
        type: "simple",
        symbol: aircraftMarker,
        visualVariables: {
            type: "rotation",
            field: "true_track",
            rotationType: "geographic"
        }
    };

    let airportsRenderer = {
        type: "simple",
        symbol: airportMarker,
    };

    let popup = new PopupTemplate ({
        title: "Aircraft information",
        content: [{
            type: "fields",
            fieldInfos: [{
                fieldName: "icao24",
                label: "Icao24"
            }, {
                fieldName: "callsign",
                label: "Callsign"
            }, {
                fieldName: "origin_country",
                label: "Origin country"
            }, {
                fieldName: "longitude",
                label: "Longitude, DD"
            }, {
                fieldName: "latitude",
                label: "Latitude, DD"
            }, {
                fieldName: "baro_altitude",
                label: "Baro altitude, m"
            }, {
                fieldName: "velocity",
                label: "Velocity, m/s"
            }, {
                fieldName: "true_track",
                label: "True track, DD"
            }]
        }],
    });

    // let lineChartValue = new ChartMediaInfoValue({
    //     fields: ["day","arrivals_quantity"],
    //     normalizeField: null,
    //     tooltipField: "<field name>"
    // });
    //
    // let lineChart = new LineChartMediaInfo({
    //     title: "<b>Arrivals</b>",
    //     caption: "Per day",
    //     value: lineChartValue
    // });

    let airportPopup = new PopupTemplate ({
        title: "Airport information",
        content: [{
            type: "fields",
            fieldInfos: [{
                fieldName: "icao",
                label: "Icao"
            }, {
                fieldName: "name",
                label: "Name"
            }, {
                fieldName: "city",
                label: "City"
            }, {
                fieldName: "country",
                label: "Country"
            }, {
                fieldName: "longitude",
                label: "Longitude, DD"
            }, {
                fieldName: "latitude",
                label: "Latitude, DD"
            }]
            // }, {
            //     type: "media",
            //     mediaInfos: [lineChart]
            // }]
        }]
    });

    let pathPopup = new PopupTemplate ({
        title: "Path information",
        content: [{
            type: "fields",
            fieldInfos: [{
                fieldName: "icao24",
                label: "Icao24"
            }, {
                fieldName: "callsign",
                label: "Callsign"
            }]
        }]
    });

    function aircraftToGraphics (aircraft) {

        let point = {
            type: "point",
            longitude: aircraft.longitude,
            latitude: aircraft.latitude
        };
        let attributes = {
            icao24: aircraft.icao24,
            callsign: aircraft.callsign,
            origin_country: aircraft.origin_country,
            longitude: aircraft.longitude,
            latitude: aircraft.latitude,
            true_track: aircraft.true_track,
            baro_altitude: aircraft.baro_altitude,
            velocity: aircraft.velocity
        };
        return new Graphic({
            geometry: point,
            symbol: aircraftMarker,
            attributes: attributes,
            popupTemplate: popup
        })
    }

    function airportToGraphics (airport) {
        let point = {
            type: "point",
            longitude: airport.longitude,
            latitude: airport.latitude
        };
        let attributes = {
            icao: airport.icao,
            city: airport.city,
            country: airport.country,
            longitude: airport.longitude,
            latitude: airport.latitude,
            name: airport.name
        };
        return new Graphic({
            geometry: point,
            symbol: airportMarker,
            attributes: attributes,
            popupTemplate: airportPopup
        })
    }

    const aircraft = {
        icao24: "XXXXX",
        callsign: "XXXXX",
        origin_country: "XXXXX",
        longitude: 50,
        latitude: 50,
        true_track: 56,
        baro_altitude: 0,
        velocity: 0
    };

    let graphics = aircraftToGraphics(aircraft);

    const airport = {
        icao: "XXXX",
        city: "XXXX",
        country: "XXXX",
        longitude: 40,
        latitude: 40,
        name: "XXXX"
    }

    let airportGraphics = airportToGraphics(airport);

    let airportFeatureLayer = new FeatureLayer({
        source: [airportGraphics],
        supportsEditing: true,
        supportsAdd: true,
        fields: airportFields,
        renderer: airportsRenderer,
        geometryType: "point",
        spatialReference: {
            wkid: 3857
        },
        outFields: ["*"],
        popupEnabled: true,
        popupTemplate: airportPopup
    });

    let featureLayer = new FeatureLayer({
        source: [graphics],
        supportsEditing: true,
        supportsAdd: true,
        fields: fields,
        renderer: rotationRenderer,
        geometryType: "point",
        spatialReference: {
            wkid: 3857
        },
        popupEnabled: true,
        popupTemplate: popup,
        outFields: ["*"]
    });

    const remainingPath = {
        type: "polyline",
        paths: [
            [90, 60], //Longitude, latitude
            [20, 53],  //Longitude, latitude
            [2, 40],  //Longitude, latitude
        ]
    };

    const traveledPath = {
        type: "polyline",
        paths: [
            [65, 55], //Longitude, latitude
            [70, 58],  //Longitude, latitude
        ]
    };

    const traveledPathSymbol = {
        type: "simple-line",
        color: [226, 119, 40], // Orange
        width: 2,
        style: "short-dot"
    };

    const traveledPathGraphic = new Graphic({
        geometry: traveledPath,
        symbol: traveledPathSymbol
    });

    const remainingPathSymbol = {
        type: "simple-line",
        color: [226, 119, 40], // Orange
        width: 2,
        style: "short-dot"
    };

    const remainingPathGraphic = new Graphic({
        geometry: remainingPath,
        symbol: remainingPathSymbol
    });

    let pathsRenderer = {
        type: "simple",
        symbol: traveledPathSymbol,
    };

    let remainingPathsRenderer = {
        type: "simple",
        symbol: remainingPathSymbol,
    };

    let remainingPathFeatureLayer = new FeatureLayer({
        source: [remainingPathGraphic],
        supportsEditing: true,
        supportsAdd: true,
        fields: fields,
        objectIdField: "objectID",
        renderer: remainingPathsRenderer,
        geometryType: "polyline",
        spatialReference: {
            wkid: 3857
        },
        popupEnabled: true,
        popupTemplate: popup
    });

    let traveledPathFeatureLayer = new FeatureLayer({
        source: [traveledPathGraphic],
        supportsEditing: true,
        supportsAdd: true,
        fields: pathFields,
        objectIdField: "objectID",
        renderer: pathsRenderer,
        geometryType: "polyline",
        spatialReference: {
            wkid: 3857
        },
        popupEnabled: true,
        popupTemplate: pathPopup
    });

    const graphic = {
        popupTemplate: {
            title: "Mouse over aircrafts to show details..."
        }
    };
    const feature = new Feature({
        container: "feature-node",
        graphic: graphic,
        map: view.map,
        spatialReference: view.spatialReference
    });

    async function applyEditsToLayer(edits, layer) {
        // console.log("applyEditsToLayer started");
        let results = await layer.applyEdits(edits);
        if (results.deleteFeatureResults.length > 0) {
            console.log(
                results.deleteFeatureResults.length,
                "object(s) have been removed"
            );
        }
        if (results.addFeatureResults.length > 0) {
            let add_finished = Date.now();
            let objectIds = [];
            results.addFeatureResults.forEach(function (item) {
                objectIds.push(item.objectId);
            });
            let res = await layer.queryFeatures({
                objectIds: objectIds
            });
            console.log(res.features.length + " object(s) have been added");

            // Object.keys(res.features[0]).forEach(key => {
            //     console.log("res.features[0][key]: " +  res.features[0][key]);
            // })

            // let time_delta = add_finished - addStarts;
            // console.log("Time spent: " + time_delta + " ms");
        }
    }

    async function removeFeatures(graphics, layer) {
        async function constructDeletes(graphics) {
            let deleteEdits = {
                deleteFeatures: []
            };
            if (graphics.length !== 0) {
                deleteEdits.deleteFeatures = graphics;
                return deleteEdits;
            } else {
                let results = await layer.queryFeatures();
                deleteEdits.deleteFeatures = results.features;
                return deleteEdits;
            }
        }
        let edits = await constructDeletes(graphics)
        await applyEditsToLayer(edits, layer);
        addStarts = Date.now();
    }

    async function addFeatures(graphics, layer) {
        let addObjects = {
            addFeatures: []
        };
        if (graphics) {
            graphics.forEach(function (item) {
                addObjects.addFeatures.push(item);
            })
        }
        await applyEditsToLayer(addObjects, layer);
    }

    map.add(airportFeatureLayer);
    removeFeatures([], airportFeatureLayer);

    map.add(featureLayer);
    removeFeatures([], featureLayer);

    map.add(remainingPathFeatureLayer);
    removeFeatures([], remainingPathFeatureLayer);

    map.add(traveledPathFeatureLayer);
    removeFeatures([], traveledPathFeatureLayer);

    watchUtils.whenTrue(view, "stationary", function() {
        if (view.extent) {
            extentCoords.xMin = parseFloat(view.extent.xmin.toFixed(2));
            extentCoords.yMin = parseFloat(view.extent.ymin.toFixed(2));
            extentCoords.xMax = parseFloat(view.extent.xmax.toFixed(2));
            extentCoords.yMax = parseFloat(view.extent.ymax.toFixed(2));
            if (Object.keys(buckets).length !== 0) {
                render(buckets, extentCoords, true)
            }
        }
    })

    view.whenLayerView(featureLayer).then(function (layerView) {
        let highlight;

        view.on("pointer-move", function (event) {
            view.hitTest(event).then(function (response) {
                let results = response.results;
                let result = results[0];
                highlight && highlight.remove();
                if (result) {
                    feature.graphic = result.graphic;
                    highlight = layerView.highlight(result.graphic);
                } else {
                    feature.graphic = graphic;
                }
            });
        });
    });

    view.whenLayerView(airportFeatureLayer).then(function (layerView) {
        let highlight;
        view.on("pointer-move", function (event) {
            view.hitTest(event).then(function (response) {
                let results = response.results;
                let result = results[0];
                highlight && highlight.remove();
                if (result) {
                    feature.graphic = result.graphic;
                    highlight = layerView.highlight(result.graphic);
                } else {
                    feature.graphic = graphic;
                }
            });
        });
    });

    function calculateHash (str, bucketsQuantity) {
        let h = 0, i = 0, len = str.length;
        if (len === 0) {
            return h;
        }
        while (i < len) {
            let char = str.charCodeAt(i);
            h = (((h << 5) - h) + char) % bucketsQuantity;
            i ++;
        }
        return (h % bucketsQuantity);
    }

    function bucketize (aircraft) {
        let buckets = [];
        for (let i = 0; i < 128; i ++) {
            buckets[i] = [];
        }
        aircraft.forEach(aircraft => {
            if (aircraft['icao24'] !== undefined) {
                let hash = calculateHash(aircraft['icao24'], Object.keys(buckets).length);
                buckets[hash].push(aircraft);
            }
        });
        return buckets
    }

    function selectPoints(buckets, extent) {
        let graphics = {};
        let icaoToRender = new Set ();
        const maxPointQuantity = 200;
        let count = 0;
        if (buckets.length !== 0) {
            buckets.forEach(bucket => {
                if (count > maxPointQuantity) {
                    return [icaoToRender, graphics];
                }
                bucket.forEach(aircraft => {
                    let long = aircraft['longitude'];
                    let lat = aircraft['latitude'];
                    let coords = webMercatorUtils.lngLatToXY(long, lat);
                    long = parseFloat(coords[0].toFixed(2));
                    lat = parseFloat(coords[1].toFixed(2));

                    if (
                        (lat > extent.yMin) && (lat < extent.yMax) &&
                        (long > extent.xMin) && (long < extent.xMax)
                    ) {
                        let id = aircraft['icao24'];
                        graphics[id] = aircraftToGraphics(aircraft);
                        count++;
                        icaoToRender.add(id);
                    }
                })
            })
        }
        return [icaoToRender, graphics];
    }

    async function render (buckets, currentExtent, partial = false) {
        let result = selectPoints(buckets, currentExtent);
        let icaos = result[0];
        let graphics = result[1];

        let deleteObjects = [];
        let addObjects = [];
        if (partial === true) {
            icaos.forEach(icao => {
                if (!(Object.keys(graphicObjects).includes(icao))) {
                    addObjects.push(graphics[icao]);
                }
            });
            Object.keys(graphicObjects).forEach(icao => {
                if (!(icaos.has(icao))) {
                    deleteObjects.push(graphicObjects[icao])
                }
            });
        } else {
            Object.keys(graphics).forEach(icao => {
                addObjects.push(graphics[icao]);
            })
        }

        // console.log("addObjects length: " + addObjects.length);
        await removeFeatures(deleteObjects, featureLayer);
        await addFeatures(addObjects, featureLayer);

        graphicObjects = graphics;
    }

    async function drawAirports (airports) {
        let addObjects = [];
        airports[1].forEach(airport => {
            let airportGraphic = airportToGraphics(airport)
            addObjects.push(airportGraphic);
        });
        await addFeatures(addObjects, airportFeatureLayer);
    }

    function updateAircraft (aircraft) {
        buckets = bucketize(aircraft);
        render(buckets, extentCoords);
    }

    function pathToGraphics (path, flight) {
        let traveledPath = {
            type: "polyline",
            paths: path
        };

        let pathAttributes = {
            icao24: flight.icao24,
            callsign: flight.callsign
        }

        return new Graphic({
            geometry: traveledPath,
            symbol: traveledPathSymbol,
            attributes: pathAttributes
        })
    }

    async function renderFlight (flight, x, y) {
        // console.log("flight:");
        // console.log(flight);

        if (flight !== undefined && flight !== null) {
            let departure_airport_icao = flight.departure_airport_icao;
            // let arrival_airport_icao = flight.arrival_airport_icao;

            // console.log("flight icao: " + flight.icao24);
            // console.log("callsign: " + flight.callsign);
            // console.log("departure airport: " + flight.departure_airport_icao);
            // console.log("arrival airport: " + flight.arrival_airport_icao);

            let traveledPath = [];
            if (flight.departure_airport_icao !== null) {
                let query = {
                    where: "icao = '" + departure_airport_icao + "'",
                    outFields: [ "longitude", "latitude"],
                    spatialRelationship: "intersects",
                    returnGeometry: true,
                };

                let response = await airportFeatureLayer.queryFeatures(query);
                let features = response.features;
                if (features.length !== 0) {
                    let long = features[0].attributes.longitude;
                    let lat = features[0].attributes.latitude;
                    // console.log("departure airport coords: " + [long, lat]);
                    traveledPath.push([long, lat]);
                }
            }

            // if (flight.arrival_airport_icao !== null) {
            //     let query = {
            //         where: "icao = '" + arrival_airport_icao + "'",
            //         outFields: [ "longitude", "latitude"],
            //         spatialRelationship: "intersects",
            //         returnGeometry: true,
            //     };
            //
            //     console.log("query.where: " +  query.where);
            //
            //     let response = await airportFeatureLayer.queryFeatures(query);
            //     let features = response.features;
            //     if (features.length !== 0) {
            //         let long = features[0].attributes.longitude;
            //         let lat = features[0].attributes.latitude;
            //         console.log("arrival airport coords: " + [long, lat]);
            //
            //         let remainingPath = []
            //         remainingPath.push([long, lat]);
            //         remainingPath.push([x, y]);
            //
            //         let remainingPathGraphic = pathToGraphics(remainingPath, flight.icao24);
            //         console.log("Let's draw the remaining path! =)");
            //         let addObjects = [];
            //         addObjects.push(remainingPathGraphic);
            //         await addFeatures(addObjects, remainingPathFeatureLayer);
            //     }
            // }
            // console.log("quantity of path points for the flight: " + flight.path.length);

            flight.path.forEach(path => {
                if (path.longitude !== null && path.latitude !== null) {
                    traveledPath.push([path.longitude, path.latitude]);
                }
            })

            if (x !== null && y !== null) {
                traveledPath.push([x, y]);
            }

            // console.log("traveledPath: " + traveledPath);

            let pathGraphic = pathToGraphics(traveledPath, flight);

            let addObjects = [];
            addObjects.push(pathGraphic);
            await removeFeatures([], traveledPathFeatureLayer);
            await addFeatures(addObjects, traveledPathFeatureLayer);
        }
    }

    $(document).ready(function(){
        const socket = io({transports: ['websocket']});

        socket.on('connect', () => {
            console.log('client: connected');
            socket.send("Ready to draw the airports");
        });

        socket.on("message", data => {
            if (data !== undefined) {
                if (data[0] === "airports") {
                    drawAirports(data);
                } else {
                    if (data[0][0] === "flight") {
                        // console.log("received flight from the server: " + data);
                        if (data[1] !== null) {
                            renderFlight(data[1], data[2], data[3]);
                        }
                    } else {
                        if (data[0] === "path-update") {
                            // console.log("client received path-update. Flight:");
                            // console.log(data[1]);
                            renderFlight(data[1], null, null);
                        } else {
                            updateAircraft(data);
                        }
                    }
                }
            }
        });

        view.whenLayerView(featureLayer).then(function (layerView) {
            view.on("click", function (event) {

                removeFeatures([], traveledPathFeatureLayer);
                // removeFeatures([], remainingPathFeatureLayer);

                let screenPoint = {
                    x: event.x,
                    y: event.y
                };

                // console.log("screenPoint.x before: " + screenPoint.x);
                // console.log("screenPoint.y before: " + screenPoint.y);

                view.hitTest(screenPoint).then(function (response) {
                    if (response.results.length !== 0) {
                        let feature = response.results.filter(result => {
                            return result.graphic.layer === featureLayer;
                        })
                        if (feature !== undefined) {
                            let graphic = feature[0].graphic;
                            let attributes = graphic.attributes;
                            let icao = attributes.icao24;

                            let mp = webMercatorUtils.webMercatorToGeographic(event.mapPoint);

                            screenPoint.x = mp.x;
                            screenPoint.y = mp.y;

                            // console.log("screenPoint.x after: " + screenPoint.x);
                            // console.log("screenPoint.y after: " + screenPoint.y);

                            socket.send(["icao24", icao, screenPoint.x, screenPoint.y])
                        }
                    }
                });
            });
        });

        async function queryPath () {
            let results = await traveledPathFeatureLayer.queryFeatures();
            let features = results.features;

            if (features.length !== 0) {
                // console.log("time interval update");
                // console.log(features[0].geometry.paths);
                let icao = features[0].attributes.icao24;
                let message = ["path-update", icao]
                socket.send(["path-update", icao]);
            }
        }

        setInterval (queryPath, 4000);
    });
})
