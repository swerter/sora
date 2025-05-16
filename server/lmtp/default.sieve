require ["fileinto", "envelope", "reject"];
if envelope :is "from" "spam@example.com" {
    discard;
} elsif header :contains "subject" "important" {
    fileinto "Important";
} else {
    keep;
}
