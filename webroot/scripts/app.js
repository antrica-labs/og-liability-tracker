var LineChart = React.createClass({
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
            <canvas id={this.props.id} ref="chartCanvas"></canvas>
        )
    }
});

var data = {
    labels: ["January", "February", "March", "April", "May", "June", "July", "August", "September"],
    datasets: [
        {
            label: "Historical",
            fill: false,
            backgroundColor: "#C0C0C0",
            borderColor: "#A0A0A0",
            data: [63259226.71999967, 57381112.25999987, 50542420.19999969, 46350201.140000105, 42194867.309999764, 36926610.67999977, 50212553.44]

        },
        {
            label: "Forecast",
            fill: false,
            backgroundColor: "#91D8D8",
            borderColor: "#4BC0C0",
            data: [null, null, null, null, null, null, 50212553.44, -15017276.718553275, -20051379.163761586]
        }
    ]
};

var options = {
    responsive: true,
    title: {
        display: true,
        text: 'Net LMR Value'
    },
    scales: {
        yAxes: [{
            labelString: "Hello"
        }]
    }
};

ReactDOM.render(
    <LineChart id="lmrChart" data={data} options={options} />
    ,    
    document.getElementById('content')
);