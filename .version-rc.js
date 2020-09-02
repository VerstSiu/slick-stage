module.exports = function() {
  var hostUrl = "https://github.com/VerstSiu/";
  var projectUrl = hostUrl + "slick-stage";

  return {
    types: [
      {type: "feat", hidden: false},
      {type: "fix", hidden: false},
      {type: "chore", hidden: true},
      {type: "docs", hidden: true},
      {type: "style", hidden: true},
      {type: "refactor", hidden: true},
      {type: "perf", hidden: true},
      {type: "test", hidden: true},
      {type: "ci", hidden: true},
      {type: "revert", hidden: true}
    ],
    preMajor: true

    commitUrlFormat: projectUrl + "commit/{{hash}}",
    compareUrlFormat: projectUrl + "compare/{{previousTag}}...{{currentTag}}",
    issueUrlFormat: projectUrl + "issues/{{id}}",
    userUrlFormat: hostUrl + "{{user}}"
  };
}