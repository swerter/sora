require ["fileinto"];

if exists "X-Spam-Flag" {
    fileinto "Junk";
} else {
    keep;
}
