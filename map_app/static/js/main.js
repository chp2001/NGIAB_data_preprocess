var wb_id_dict = {};
var selected_wb_layer = null;
var upstream_maps = {};
var flowline_layers = {};

var registered_layers = {}

async function update_selected() {
    console.log('updating selected');
    if (!(Object.keys(wb_id_dict).length === 0)) {
        return fetch('/get_geojson_from_wbids', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(wb_id_dict),
        })
            .then(response => response.json())
            .then(data => {
                // if the wb_id is already in the dict, remove the key
                // remove the old layer
                if (selected_wb_layer) {
                    map.removeLayer(selected_wb_layer);
                }
                console.log(data);
                // add the new layer
                selected_wb_layer = L.geoJSON(data).addTo(map);
                selected_wb_layer.eachLayer(function (layer) {
                    layer._path.classList.add('selected-wb-layer');
                });
            })
            .catch(error => {
                console.error('Error:', error);
            });
    } else {
        if (selected_wb_layer) {
            map.removeLayer(selected_wb_layer);
        }
        return Promise.resolve();
    }
}

async function populate_upstream() {
    console.log('populating upstream selected');
    // drop any key that is not in the wb_id_dict
    for (const [key, value] of Object.entries(upstream_maps)) {
        if (!(key in wb_id_dict)) {
            map.removeLayer(value);
            delete upstream_maps[key];
        }
    }
    // add any key that is in the wb_id_dict but not in the upstream_maps
    for (const [key, value] of Object.entries(wb_id_dict)) {
        if (!(key in upstream_maps)) {
            upstream_maps[key] = null;
        }
    }
    if (Object.keys(upstream_maps).length === 0) {
        return Promise.resolve();
    }

    const fetchPromises = Object.entries(upstream_maps).map(([key, value]) => {
        if (value === null) {
            return fetch('/get_upstream_geojson_from_wbids', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(key),
            })
                .then(response => response.json())
                .then(data => {
                    // if the wb_id is already in the dict, remove the key
                    // remove the old layer
                    if (upstream_maps[key]) {
                        map.removeLayer(upstream_maps[key]);
                    }
                    console.log(data);
                    // add the new layer if the downstream wb's still selected
                    if (key in wb_id_dict) {
                        layer_group = L.geoJSON(data).addTo(map);
                        upstream_maps[key] = layer_group;
                        layer_group.eachLayer(function (layer) {
                            if (layer._path) {
                                layer._path.classList.add('upstream-wb-layer');
                            }
                        });
                    }
                })
                .catch(error => {
                    console.error('Error:', error);
                });
        }
    });
    return fetchPromises;

}

async function populate_flowlines() {
    console.log('populating flowlines');
    // drop any key that is not in the wb_id_dict
    for (const [key, value] of Object.entries(flowline_layers)) {
        if (!(key in wb_id_dict)) {
            for (i of flowline_layers[key]) {
                map.removeLayer(i);
                delete flowline_layers[key];
            }
        }
    }
    // add any key that is in the wb_id_dict but not in the flowline_layers
    for (const [key, value] of Object.entries(wb_id_dict)) {
        if (!(key in flowline_layers)) {
            flowline_layers[key] = null;
        }
    }
    if (Object.keys(flowline_layers).length === 0) {
        return Promise.resolve();
    }

    const fetchPromises = Object.entries(flowline_layers).map(([key, value]) => {
        if (value === null) {
            return fetch('/get_flowlines_from_wbids', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(key),
            })
                .then(response => response.json())
                .then(data => {
                    // if the wb_id is already in the dict, remove the key
                    // remove the old layer
                    if (flowline_layers[key]) {
                        for (i of flowline_layers[key]) {
                            map.removeLayer(i);
                        }
                    }
                    // loud!
                    // console.log(data);
                    to_wb = JSON.parse(data['to_wb']);
                    to_nexus = JSON.parse(data['to_nexus']);
                    nexus = JSON.parse(data['nexus']);
                    // add the new layer if the downstream wb's still selected
                    if (key in wb_id_dict) {
                        to_wb_layer = L.geoJSON(to_wb).addTo(map);
                        to_nexus_layer = L.geoJSON(to_nexus).addTo(map);
                        nexus_layer = L.geoJSON(nexus).addTo(map);
                        // hack to add css classes to the flowline layers
                        // using eachLayer as it waits for layer to be done updating
                        // directly accessing the _layers keys may not always work
                        to_wb_layer.eachLayer(function (layer) {
                            if (layer._path) {
                                layer._path.classList.add('flowline-to-wb-layer');
                            }
                        });
                        to_nexus_layer.eachLayer(function (layer) {
                            if (layer._path) {
                                layer._path.classList.add('flowline-to-nexus-layer');
                            }
                        });
                    }
                    flowline_layers[key] = [to_wb_layer, to_nexus_layer, nexus_layer];
                })

                .catch(error => {
                    console.error('Error:', error);
                });
        }
    });
    return fetchPromises;

}

async function synchronizeUpdates() {
    console.log('Starting updates');

    // wait for all promises
    const upstreamPromises = await populate_upstream();
    const flowlinePromises = await populate_flowlines();
    const selectedPromise = await update_selected();
    await Promise.all([selectedPromise, ...upstreamPromises, ...flowlinePromises]).then(() => {
        // This block executes after all promises from populate_upstream and populate_flowlines have resolved
        console.log('All updates are complete');
        // BringToFront operations or any other operations to perform after updates
        if (selected_wb_layer) {
            selected_wb_layer.bringToFront();
        }
        for (const [key, value] of Object.entries(flowline_layers)) {
            if (key in wb_id_dict) {
                value[0].bringToFront();
                value[1].bringToFront();
            }
        }
    }).catch(error => {
        console.error('An error occurred:', error);
    });
}

function onMapClick(event) {
    // Extract the clicked coordinates
    var lat = event.latlng.lat;
    var lng = event.latlng.lng;

    // Send an AJAX request to the Flask backend
    fetch('/handle_map_interaction', {
        method: 'POST',
        headers: {
            'Content-Type': 'application/json',
        },
        body: JSON.stringify({
            coordinates: { lat: lat, lng: lng }
        }),
    })
        .then(response => response.json())
        .then(data => {
            // if the wb_id is already in the dict, remove the key
            if (data['wb_id'] in wb_id_dict) {
                delete wb_id_dict[data['wb_id']];
            }
            else {
                wb_id_dict[data['wb_id']] = [lat, lng];
            }
            console.log('clicked on wb_id: ' + data['wb_id'] + ' coords :' + lat + ', ' + lng);
            synchronizeUpdates();
            $('#selected-basins').text(Object.keys(wb_id_dict).join(', '));

        })
        .catch(error => {
            console.error('Error:', error);
        });

}

async function get_boundary(vpu_code) {
    // calculate the bounds from the grid bounds
    const proxyUrl = '/get_map_data';
    url = baseUrl + vpu_code + '@png/18/0/0.png';
    // tile url will 404 if the tile doesn't exist, and then return the bounds in the body
    //e.g. Coverage [minx,miny,maxx,maxy] is [53410, 154705, 65609, 162724, 18], index [x,y,z] is [0, 0, 18]

    try {
        const response = await fetch(`${proxyUrl}?url=${encodeURIComponent(url)}`);
        bounds = await response.json();
    } catch (error) {
        console.error('Error fetching boundary:', error);
    }
    // regex to find the bounds ([0-9]+,) match 0-4 are xmin, ymin, xmax, ymax
    // convert to lat long
    var xmin = bounds[0];
    var ymin = bounds[1];
    var xmax = bounds[2];
    var ymax = bounds[3];
    return L.latLngBounds(L.latLng(ymin, xmin), L.latLng(ymax, xmax));
}

async function addLayers() {
    await Promise.all(Object.keys(geometry_urls).map(async (key) => {
        var geometryUrl = 'HS-' + geometry_urls[key] + ':' + key + '_boundaries@EPSG:900913';
        bounds = await get_boundary(geometryUrl);
        L.tileLayer(baseUrl + geometryUrl + '@png/{z}/{x}/{-y}.png', {
            transparent: true,
            format: 'image/png',
            opacity: .5,
            minZoom: 8,
            maxZoom: 18,
            reuseTiles: true,
            bounds: bounds,
        }).addTo(map);
    }));
}

async function preload_zarrs() {
    original_text = document.getElementById('forcings-button').textContent;
    document.getElementById('forcings-button').disabled = true;
    document.getElementById('forcings-button').textContent = "Preloading Zarr Metadata";

    document.getElementById('forcings-loading').style.visibility = "visible";
    const zarrs = await fetch('/preload_zarrs', {
        method: 'GET',
        headers: { 'Content-Type': 'application/json' },
    })
        .then(_ => {
            document.getElementById('forcings-button').disabled = false;
            document.getElementById('forcings-loading').style.visibility = "hidden";
            document.getElementById('forcings-button').textContent = original_text;
        });
}



geometry_urls = {
    '16': 'e8ddee6a8a90484fa7a976458e79c0c3',
    '01': '5f0e81c665314967a1e15e4ae672aaae',
    '02': '131a6d6cc6514b558f968716783d7d47',
    '03N': '38c84132987243c2a49ffb9d178f3162',
    '03S': '5d9cdd0b6851460aaccd0c83557e4a6c',
    '03W': '5674050a194c41b8a61f000c94c27983',
    '04': 'd161033e07634d6199ae136a24807f22',
    '05': '47113551c63b41daa53465aee6cb69e9',
    '06': '1302f07176cd46e2ab70db730e601682',
    '07': 'b380393bebaf47e68afd98fb15f4ff10',
    '08': '2391aadf1f4440499e7b61b4dcc41d94',
    '09': '27670ef43fbf42be914e1fca7d41ce0b',
    '10L': 'b5028b1c8b5240f8b7deb3bcebc2f005',
    '10U': 'b6dca803df5a4a8c8120512ccdfe8ba9',
    '11': '8e7a4c951c8241269e47ee461c1d9ef3',
    '12': '8ea1c9e098f044318777bf283c1fc0ad',
    '13': 'b166308dffed4db39083393a894c3694',
    '15': '68501dc3b6214aca8d92aaae75aee941',
    '16': '1244ac2f25b0442cacece320424c6756',
    '17': 'da20b06af50d4adab080597ae4ae8c46',
    '18': 'ca2e56965245476fbcb258b7d2aec7ab',
    '14': '2d78b60ad0cf469daced4c4aa37764ad',
}

// Initialize the map
var map = L.map('map').setView([40, -96], 5);

//Create in-map Legend / Control Panel
var legend = L.control({ position: 'bottomright' });
// load in html template for the legend
legend.onAdd = function (map) {
    legend_div = L.DomUtil.create('div', 'custom_legend');
    return legend_div
};
legend.addTo(map);


// Add OpenStreetMap tiles to the map
L.tileLayer('https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png', {
    maxZoom: 18,
    attribution: '© OpenStreetMap contributors',
    crs: L.CRS.EPSG3857
}).addTo(map);


var baseUrl = "https://geoserver.hydroshare.org/geoserver/gwc/service/tms/1.0.0/";  // Base URL of the WMTS service

var boundaries_of_vpus = "HS-35e8c6023c154b6298fcda280beda849:vpu_boundaries@EPSG:900913";

southWest = L.latLng(22.5470, -129.4137);
northEast = L.latLng(51.0159, -68.9337);
bounds = L.latLngBounds(southWest, northEast);

var wmtsLayer = L.tileLayer(baseUrl +
    boundaries_of_vpus + '@png/{z}/{x}/{-y}.png', {
    attribution: '&copy; <a href="https://www.hydroshare.org/">Hydroshare</a> contributors',
    transparent: true,
    format: 'image/png',
    opacity: 1,
    maxZoom: 7,
    bounds: bounds,
}).addTo(map);

addLayers().then(() => {
    console.log('added layers');
    map.on('click', onMapClick);
});

// preload_zarrs().then((zarrs) => {
//     console.log('preloaded zarrs');
//     console.log(zarrs);
// });



