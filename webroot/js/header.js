var Navbar = React.createClass({
    downloadReport: function() {
        if (this.state.report.report_date != undefined) {
            var url = '/api/export_historical_report?province_id=1&report_date=' + moment(this.state.report.report_date).format("YYYY-MM-DD");

            location.href = url;
        }
    },
    displayRating: function(report) {
        if (report.rating != undefined)
            return parseFloat(report.rating).toFixed(2);
        else
            return 'No reports';
    },
    displayMonth: function(report) {
        if (report.report_date != undefined)
            return '(' + moment(report.report_date).format("MMMM YYYY") + ')';
        else
            return '';
    },
    loadLatestReport: function() {
        $.ajax({
            url: this.props.latestReportUrl,
            dataType: 'json',
            cache: false,
            success: function(data) {
                this.setState({ report: data });
            }.bind(this),
            error: function(xhr, status, err) {
                console.error(this.props.url, status, err.toString());
            }.bind(this)
        });
    },
    getInitialState: function() {
        return { report : {} };
    },
    componentDidMount: function() {
        this.loadLatestReport();
        //setInterval(this.loadLatestReport, this.props.pollInterval);
    },
    render: function() {
        return (
            <ul className="nav navbar-nav navbar-right">
                <li><span className="navbar-text">Latest Rating:</span></li>
                <li className="dropdown" id="rating">
                    <a href="#" className="dropdown-toggle" data-toggle="dropdown" role="button">{this.displayRating(this.state.report)} {this.displayMonth(this.state.report)} <span className="caret"></span></a>
                    <ul className="dropdown-menu">
                        <li><a href="#" onClick={this.downloadReport}>Export latest details</a></li>
                    </ul>
                </li>
            </ul>
        );
    }
});

ReactDOM.render(
    <Navbar latestReportUrl="/api/latest_report_date?province_id=1" pollInterval={5000} />
    ,
    document.getElementById('navbar')
);