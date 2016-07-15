var PROVINCE = 1;

var netValueOptions = {
    responsive: true,
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
                            lineTension: 0,
                            backgroundColor: "#CDA694",
                            borderColor: "#AD5755",
                            data: data.historical
                        },
                        {
                            label: "Base forecast",
                            fill: false,
                            lineTension: 0,
                            backgroundColor: "#CEB675",
                            borderColor: "#AD9962",
                            data: data.base_forecast
                        },
                        {
                            label: "Adjusted Forecast",
                            fill: false,
                            lineTension: 0,
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
                    <DivestmentList listUrl={"/api/dispositions?province_id=" + PROVINCE} pollInterval={60000} />
                    <AcquisitionList listUrl={"/api/acquisitions?province_id=" + PROVINCE} pollInterval={60000} />
                    <GrowthProjectsList listUrl={"/api/growth_entities?province_id=" + PROVINCE} pollInterval={60000} />
                    <AroList listUrl={"/api/aro_plans?province_id=" + PROVINCE} pollInterval={60000} />
                </div>
            </div>
        )
    }
});

var AcquisitionList = React.createClass({
    loadLatest: function() {
        $.ajax({
            url: this.props.listUrl,
            dataType: 'json',
            cache: false,
            success: function(data) {
                this.setState({ data: data });
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(this.props.url, status, err.toString());
            }.bind(this)
        });
    },
    getInitialState: function() {
        return { data : [] } ;
    },
    componentDidMount: function() {
        this.loadLatest();
        //setInterval(this.loadLatest, this.props.pollInterval);
    },
    render: function () {
        var nodes = this.state.data.map(function(item) {
            return (
               <li className="optional acquisition" key={item.id}>
                   <span className="description">{item.description}</span>
                   <span className="effective_date">{moment(item.effective_date).format("MMMM D, YYYY")}</span>
                   <span className="purchase_price">${parseFloat(item.purchase_price).toFixed(2)}</span>
               </li>
            );
        });

        return (
            <div id="acquisition_list" className="col-md-3 toggle-list">
                <h4>Acquisitions</h4>

                <ul className="toggles">
                    {nodes}
                </ul>
            </div>
        )
    }
});

var DivestmentList = React.createClass({
    loadLatest: function() {
        $.ajax({
            url: this.props.listUrl,
            dataType: 'json',
            cache: false,
            success: function(data) {
                this.setState({ data: data });
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(this.props.url, status, err.toString());
            }.bind(this)
        });
    },
    getInitialState: function() {
        return { data : [] } ;
    },
    componentDidMount: function() {
        this.loadLatest();
        //setInterval(this.loadLatest, this.props.pollInterval);
    },
    render: function () {
        var nodes = this.state.data.map(function(item) {
            return (
                <li className="optional acquisition" key={item.id}>
                    <span className="description">{item.description}</span>
                    <span className="effective_date">{moment(item.effective_date).format("MMMM D, YYYY")}</span>
                    <span className="sale_price">${parseFloat(item.sale_price).toFixed(2)}</span>
                    <span className="licence_count">{item.licence_count} licences</span>
                </li>
            );
        });

        return (
            <div id="divestment_list" className="col-md-3 toggle-list">
                <h4>Licence Transfers</h4>

                <ul className="toggles">
                    {nodes}
                </ul>
            </div>
        )
    }
});

var GrowthProjectsList = React.createClass({
    loadLatest: function() {
        $.ajax({
            url: this.props.listUrl,
            dataType: 'json',
            cache: false,
            success: function(data) {
                this.setState({ data: data });
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(this.props.url, status, err.toString());
            }.bind(this)
        });
    },
    getInitialState: function() {
        return { data : [] } ;
    },
    componentDidMount: function() {
        this.loadLatest();
        //setInterval(this.loadLatest, this.props.pollInterval);
    },
    render: function () {
        var nodes = this.state.data.map(function(item) {
            return (
                <li className="optional acquisition" key={item.id}>
                    <span className="description">{item.entity_name}</span>
                    <span className="effective_date">{moment(item.start_date).format("MMMM D, YYYY")}</span>
                </li>
            );
        });

        return (
            <div id="groth_project_list" className="col-md-3 toggle-list">
                <h4>Growth Projects</h4>

                <ul className="toggles">
                    {nodes}
                </ul>
            </div>
        )
    }
});

var AroList = React.createClass({
    loadLatest: function() {
        $.ajax({
            url: this.props.listUrl,
            dataType: 'json',
            cache: false,
            success: function(data) {
                this.setState({ data: data });
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(this.props.url, status, err.toString());
            }.bind(this)
        });
    },
    getInitialState: function() {
        return { data : [] } ;
    },
    componentDidMount: function() {
        this.loadLatest();
        //setInterval(this.loadLatest, this.props.pollInterval);
    },
    render: function () {
        var nodes = this.state.data.map(function(item) {
            return (
                <li className="optional acquisition" key={item.id}>
                    <span className="description">{item.description}</span>
                    <span className="effective_date">{moment(item.effective_date).format("MMMM D, YYYY")}</span>
                    <span className="cost">${parseFloat(item.cost).toFixed(2)}</span>
                </li>
            );
        });

        return (
            <div id="aro_project_list" className="col-md-3 toggle-list">
                <h4>ARO Projects</h4>

                <ul className="toggles">
                    {nodes}
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