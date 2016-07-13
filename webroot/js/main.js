var netValueData = {
    labels: ["January", "February", "March", "April", "May", "June", "July", "August", "September"],
    datasets: [
        {
            label: "Historical",
            fill: false,
            backgroundColor: "#CDA694",
            borderColor: "#AD5755",
            data: [63259226.71999967, 57381112.25999987, 50542420.19999969, 46350201.140000105, 42194867.309999764, 36926610.67999977, 50212553.44]

        },
        {
            label: "Forecast",
            fill: false,
            backgroundColor: "#91D8D8",
            borderColor: "#52B2C0",
            data: [null, null, null, null, null, null, 50212553.44, -15017276.718553275, -20051379.163761586]
        }
    ]
};

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
    }
};

var LmrOverview = React.createClass({
    render: function() {
        return (
            <div>
                <NetLmrGraph id="netLmrGraph" data={netValueData} options={netValueOptions} />
                <LmrInputs />
            </div>
        );
    }
});

var NetLmrGraph = React.createClass({
    getIntialState: function() {
        return { data: [], options: [] };
    },
    componentDidMount: function() {
        var canvas = this.refs.chartCanvas;

        var chart = new Chart(canvas, {
            type: 'line',
            display: false,
            data: this.props.data,
            options: this.props.options
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
                    <AcquisitionList />
                    <DivestmentList />
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
                            <input type="text" className="form-control" value="Non-Op buy out" readOnly="true" />
                            <span className="input-group-addon"><a href="#" className="delete"><span className="glyphicon glyphicon-remove"></span></a></span>
                        </div>
                    </li>
                    <li className="optional">
                        <div className="input-group">
                            <span className="input-group-addon"><input type="checkbox" className="activation" /></span>
                            <input type="text" className="form-control" value="Granite Wash" readOnly="true" />
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