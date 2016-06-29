var map = new ol.Map({
	target: 'map',
	layers: [
 	 	new ol.layer.Tile({
			source: new ol.source.MapQuest({layer: 'sat'})
  		})
	],
	view: new ol.View({
		  center: ol.proj.fromLonLat([7.41, 50.82]),
		  zoom: 4
	})
});