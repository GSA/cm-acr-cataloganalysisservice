application_environments{
  development{
    short_name = "dev"
    long_name = "Development"
    k8s_context = "development"
  }
  test{
    short_name = "test"
    long_name = "Test"
    k8s_context = "test"
  }
  staging{
    short_name = "staging"
    long_name = "Staging"
    k8s_context = "staging"
  }
  production{
    short_name = "prod"
    long_name = "Production"
    k8s_context = "production"
  }
}

keywords{
    prod        =  /^[Pp]roduction$/
    develop     =  /^[Dd]evelop(ment|er|)$/
    stagingenv  =  /^[Ss]taging$/
    testing     =  /^[Tt]est$/
    hotfix      =  /^[Hh]ot[Ff]ix-.*/
    release     =  /^[Rr]elease-(\d+.)*\d$/
    feature     =  /^[Ff]eature.*/
    acrepo4264  =  /^feature\/ACREPO-4265/
}

libraries{
  @override mcaas_docker {
      no_cache = true
      build_strategy = "dockerfile"
      remove_local_image = false
      registry_type = "ecr"
      registry = "183925141410.dkr.ecr.us-east-1.amazonaws.com"
      additional_ecr_account_ids = "752281881774"
      cred = "artifactory-creds"
      assume_role = "arn:aws:iam::183925141410:role/acr-jenkins-multitenant-agent-serviceaccount"
  }
  
  sonarqube{
      installation_name = "sonarqube-ce"
      credential_id = "prod-sonarqube-ce"
      wait_for_quality_gate = false
      unstash = ["workspace"]
  }
  
  mcaas_stackrox
  
  git {
      github_enterprise
  }

  sdp {
      images{
        registry = "https://dtr-1.prod-iae.bsp.gsa.gov"
        repository = "docker-datacenter"
        cred = "dockerhub-creds"
      }
  }
}
