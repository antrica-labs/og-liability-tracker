var PROVINCE = 1

var netValueOptions = {
    responsive: true,
    tooltipTemplate: "$<%=addCommas(value)%>",
    title: {
        display: true,
        text: 'Net LMR Value [(Asset Value) - (Liability Amount)]'
    },
    scales: {
        yAxes: [{
            ticks: {
                callback: function (value, index, values) {
                    return '$' + parseFloat(value).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",");
                }
            }
        }]
    },
    tooltips: {
        callbacks: {
            label: function(tooltipItem, data) {
                var value = data.datasets[tooltipItem.datasetIndex].data[tooltipItem.index];

                return  ' $' + Math.round(parseFloat(value)).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ",")
            }
        }
    }
};

var LmrOverview = React.createClass({
    render: function() {
        return (
            <div>
                <CombinedLmrForecastGraph id="netLmrGraph" options={netValueOptions} url={"/api/combined_lmr_trend?province_id=" + PROVINCE} />
                <LmrInputs />
            </div>
        );
    }
});

var CombinedLmrForecastGraph = React.createClass({
    getIntialState: function() {
        return { data: [], options: [] };
    },
    getLabelsFromLmrData: function (lmrData) {
        var labels = [];

        for (var index = 0; index < lmrData.history.length; index++) {
            var reportDate = moment(lmrData.history[index].report_date);

            labels.push(reportDate.format("MMMM YYYY"));
        }

        for (var index = 0; index < lmrData.base_forecast.length; index++) {
            var reportDate = moment(lmrData.base_forecast[index].report_date);

            labels.push(reportDate.format("MMMM YYYY"));
        }


        return labels
    },
    getPointsFromLmrData: function(lmrData) {
        var historical = [];
        var base_forecast = [];
        var adjusted_forecast = [];

        for (var h = 0; h < lmrData.history.length; h++) {
            historical[h] = lmrData.history[h].net_value
        }

        for (var i = 0; i < historical.length; i++) {
            base_forecast.push(null);
            adjusted_forecast.push(null);
        }

        for (var j = 0; j < lmrData.base_forecast.length; j++) {
            base_forecast.push(lmrData.base_forecast[j].net_value);
        }

        for (var k = 0; k < lmrData.adjusted_forecast.length; k++) {
            adjusted_forecast.push(lmrData.adjusted_forecast[k].net_value);
        }

        return {
            historical: historical,
            base_forecast: base_forecast,
            adjusted_forecast: adjusted_forecast
        };
    },
    componentDidMount: function() {
        $.ajax({
            url: this.props.url,
            dataType: 'json',
            cache: false,
            success: function(lmr) {
                var labels = this.getLabelsFromLmrData(lmr);
                var data = this.getPointsFromLmrData(lmr);

                // Set the first forecast to be the same as the last history data point in order to connect the graph
                data.base_forecast[data.historical.length - 1] = data.historical[data.historical.length - 1];
                data.adjusted_forecast[data.historical.length - 1] = data.historical[data.historical.length - 1];

                var netValueData = {
                    labels: labels,
                    datasets: [
                        {
                            label: "Historical",
                            fill: false,
                            backgroundColor: "#CDA694",
                            borderColor: "#AD5755",
                            data: data.historical
                        },
                        {
                            label: "Base forecast",
                            fill: false,
                            backgroundColor: "#CEB675",
                            borderColor: "#AD9962",
                            data: data.base_forecast
                        },
                        {
                            label: "Adjusted Forecast",
                            fill: false,
                            backgroundColor: "#91D8D8",
                            borderColor: "#52B2C0",
                            data: data.adjusted_forecast
                        }
                    ]
                };

                var canvas = this.refs.chartCanvas;

                var chart = new Chart(canvas, {
                    type: 'line',
                    display: false,
                    data: netValueData,
                    options: this.props.options
                });
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(this.props.url, status, err.toString());
            }.bind(this)
        });
    },
    loadGraph: function() {

    },
    render: function() {
        return (
            <canvas id={this.props.id} ref="chartCanvas" height={130}></canvas>
        )
    }
});

var LmrInputs = React.createClass({
    render: function() {
        return (
            <div id="lmr_toggles">
                <div className="row">
                    <DivestmentList />
                    <AcquisitionList />
                    <GrowthProjectsList />
                    <AroList />
                </div>
            </div>
        )
    }
});

var AcquisitionList = React.createClass({
    render: function () {
        return (
            <div id="acquisition_list" className="col-md-3 toggle-list">
                <h4>Acquisitions</h4>

                <button type="button" className="btn btn-default">Add acquisition</button>

                <ul className="toggles">
                    <li className="optional">
                        <div className="input-group">
                            <span className="input-group-addon"><input type="checkbox" className="activation" /></span>
                            <input type="text" className="form-control" value="Non-Op Buy Outs" readOnly="true" />
                            <span className="input-group-addon"><a href="#" className="delete"><span className="glyphicon glyphicon-remove"></span></a></span>
                        </div>
                    </li>
                    <li className="optional">
                        <div className="input-group">
                            <span className="input-group-addon"><input type="checkbox" className="activation" /></span>
                            <input type="text" className="form-control" value="Shadow Creek Project" readOnly="true" />
                            <span className="input-group-addon"><a href="#" className="delete"><span className="glyphicon glyphicon-remove"></span></a></span>
                        </div>
                    </li>
                </ul>
            </div>
        )
    }
});

var DivestmentList = React.createClass({
    render: function () {
        return (
            <div id="divestment_list" className="col-md-3 toggle-list">
                <h4>Divestments</h4>

                <button type="button" className="btn btn-default">Add divestment</button>

                <ul className="toggles">
                    <li className="optional">
                        <div className="input-group">
                            <span className="input-group-addon"><input type="checkbox" className="activation" /></span>
                            <input type="text" className="form-control" value="Dixonville transfer" readOnly="true" />
                            <span className="input-group-addon"><a href="#" className="delete"><span className="glyphicon glyphicon-remove"></span></a></span>
                        </div>
                    </li>
                </ul>
            </div>
        )
    }
});

var GrowthProjectsList = React.createClass({
    render: function () {
        return (
            <div id="groth_project_list" className="col-md-3 toggle-list">
                <h4>Growth Projects</h4>
            </div>
        )
    }
});

var AroList = React.createClass({
    render: function () {
        return (
            <div id="aro_project_list" className="col-md-3 toggle-list">
                <h4>ARO Projects</h4>

                <button type="button" className="btn btn-default">Add project</button>

                <ul className="toggles">
                    <li className="optional">
                        <div className="input-group">
                            <span className="input-group-addon"><input type="checkbox" className="activation" /></span>
                            <input type="text" className="form-control" value="Administration Projects" readOnly="true" />
                            <span className="input-group-addon"><a href="#" className="delete"><span className="glyphicon glyphicon-remove"></span></a></span>
                        </div>
                    </li>
                </ul>
            </div>
        )
    }
});

ReactDOM.render(
    <LmrOverview />
    ,    
    document.getElementById('content')
);