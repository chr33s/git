import { HttpClient, HttpClientRequest } from "effect/unstable/http";

/** Hosts strip one transport suffix; preserve a literal suffix in the storage name. */
export const repositoryPath = (repo: string): string =>
  `/${encodeURIComponent(repo.endsWith(".git") ? `${repo}.git` : repo)}`;

/** HttpApi applies this to endpoint paths before prepending its base URL. */
export const repositoryClient = (client: HttpClient.HttpClient): HttpClient.HttpClient =>
  client.pipe(
    HttpClient.mapRequest((request) =>
      HttpClientRequest.setUrl(
        request,
        request.url.replace(/^\/([^/]+)/, (path) => (path.endsWith(".git") ? `${path}.git` : path)),
      ),
    ),
  );
