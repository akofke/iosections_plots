plot = document.querySelector('.plotly-graph-div');
plot.on('plotly_click', function(data) {
    var point = data['points'][0];
    var pointNum = point['pointNumber'];
    var text = point['data']['text'][pointNum];
    var ids = text.split(",");
    console.log(ids)
});