var records;
var currentPage;

$(document).ready(function () {
    $("#searchbutton").click(function (e) {
        displayModal();
    });

    $("#searchfield").keydown(function (e) {
        if (e.keyCode == 13) {
            displayModal();
        }
    });

    function displayModal() {
        $("#myModal").modal('show');
        $("#status").html("Searching...");
        $("#dialogtitle").html("Search for: " + $("#searchfield").val());
        $("#previous").hide();
        $("#next").hide();
        $.getJSON('/search/' + $("#searchfield").val(), function (data) {
            renderQueryResults(data);
        });
    }
    
    function displayImgs() {
        for(var i = 4; i > 0; i--) {
            var max = currentPage * 4;
            if (records.results[max - i] !== undefined) {
                $("#photo" + (4 - i)).html("<img src=\"" + records.results[max - i] + "\" style=\"max-width: 200px; max-height: 200px\" />");
            }
            else {
                $("#photo" + (4 - i)).html("&nbsp;");
            }
        }
    }

    $("#next").click(function (e) {
        //e.preventDefault();
        currentPage++;
        console.log("Current Page: " + currentPage + "\n");
        if ((currentPage * 4) > records.num_results) {
            $("#next").hide();
        }
        $("#previous").show();
        displayImgs();
    });

    $("#previous").click(function (e) {
        //e.preventDefault();
        currentPage--;
        console.log("Current Page: " + currentPage + "\n");
        if ((currentPage * 4) <= 4) {
            $("#previous").hide();
        }
        $("#next").show();
        displayImgs();
    });

    function renderQueryResults(data) {
        if (data.error !== undefined) {
            $("#status").html("Error: " + data.error);
        } else {
            records = data;
            currentPage = 1;
            console.log("Current Page: " + currentPage + "\n");
            $("#status").html("" + data.num_results + " result(s)");
            if (data.num_results > 4) {
                $("#next").show();
            }
            displayImgs();
        }
    }
});
