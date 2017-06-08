plot = document.querySelector('.plotly-graph-div');
plot.on('plotly_click', function(data) {
    var point = data['points'][0];
    var pointNum = point['pointNumber'];
    var text = point['data']['text'][pointNum];
    var ids = text.split(",");
    console.log(ids);
    var url = `https://metrics-dev.ccr.buffalo.edu/#job_viewer?action=search&realm=SUPREMM&resource_id=${ids[0]}&local_job_id=${ids[1]}&infoid=6&tsid=lnet`;
    var url2 = "http://pcp2.ccr.buffalo.edu:8080/supremm_rest/loader.html?resource_id=" + ids[0] + "&jobid=" + ids[1];
    window.open(url)
    window.open(url2)
});
