package graph

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path"
	"strings"

	log "github.com/Sirupsen/logrus"
	"github.com/docker/docker/engine"
	"github.com/docker/docker/image"
	"github.com/docker/docker/pkg/archive"
	"github.com/docker/docker/registry"
	"github.com/docker/docker/utils"
)

// Retrieve the all the images to be uploaded in the correct order
func (s *TagStore) getImageList(localRepo map[string]string, requestedTag string) ([]string, map[string][]string, error) {
	var (
		imageList   []string
		imagesSeen  = make(map[string]bool)
		tagsByImage = make(map[string][]string)
	)

	for tag, id := range localRepo {
		if requestedTag != "" && requestedTag != tag {
			continue
		}
		var imageListForThisTag []string

		tagsByImage[id] = append(tagsByImage[id], tag)

		for img, err := s.graph.Get(id); img != nil; img, err = img.GetParent() {
			if err != nil {
				return nil, nil, err
			}

			if imagesSeen[img.ID] {
				// This image is already on the list, we can ignore it and all its parents
				break
			}

			imagesSeen[img.ID] = true
			imageListForThisTag = append(imageListForThisTag, img.ID)
		}

		// reverse the image list for this tag (so the "most"-parent image is first)
		for i, j := 0, len(imageListForThisTag)-1; i < j; i, j = i+1, j-1 {
			imageListForThisTag[i], imageListForThisTag[j] = imageListForThisTag[j], imageListForThisTag[i]
		}

		// append to main image list
		imageList = append(imageList, imageListForThisTag...)
	}
	if len(imageList) == 0 {
		return nil, nil, fmt.Errorf("No images found for the requested repository / tag")
	}
	log.Debugf("Image list: %v", imageList)
	log.Debugf("Tags by image: %v", tagsByImage)

	return imageList, tagsByImage, nil
}

func (s *TagStore) pushRepository(r *registry.Session, out io.Writer, localName, remoteName string, localRepo map[string]string, tag string, sf *utils.StreamFormatter) error {
	out = utils.NewWriteFlusher(out)
	log.Debugf("Local repo: %s", localRepo)
	imgList, tagsByImage, err := s.getImageList(localRepo, tag)
	if err != nil {
		return err
	}

	out.Write(sf.FormatStatus("", "Sending image list"))

	var (
		repoData   *registry.RepositoryData
		imageIndex []*registry.ImgData
	)

	for _, imgId := range imgList {
		if tags, exists := tagsByImage[imgId]; exists {
			// If an image has tags you must add an entry in the image index
			// for each tag
			for _, tag := range tags {
				imageIndex = append(imageIndex, &registry.ImgData{
					ID:  imgId,
					Tag: tag,
				})
			}
		} else {
			// If the image does not have a tag it still needs to be sent to the
			// registry with an empty tag so that it is accociated with the repository
			imageIndex = append(imageIndex, &registry.ImgData{
				ID:  imgId,
				Tag: "",
			})

		}
	}

	log.Debugf("Preparing to push %s with the following images and tags", localRepo)
	for _, data := range imageIndex {
		log.Debugf("Pushing ID: %s with Tag: %s", data.ID, data.Tag)
	}

	// Register all the images in a repository with the registry
	// If an image is not in this list it will not be associated with the repository
	repoData, err = r.PushImageJSONIndex(remoteName, imageIndex, false, nil)
	if err != nil {
		return err
	}

	nTag := 1
	if tag == "" {
		nTag = len(localRepo)
	}
	for _, ep := range repoData.Endpoints {
		out.Write(sf.FormatStatus("", "Pushing repository %s (%d tags)", localName, nTag))
		for _, imgId := range imgList {
			if err := r.LookupRemoteImage(imgId, ep, repoData.Tokens); err != nil {
				log.Errorf("Error in LookupRemoteImage: %s", err)
				if _, err := s.pushImage(r, out, remoteName, imgId, ep, repoData.Tokens, sf); err != nil {
					// FIXME: Continue on error?
					return err
				}
			} else {
				out.Write(sf.FormatStatus("", "Image %s already pushed, skipping", utils.TruncateID(imgId)))
			}
			for _, tag := range tagsByImage[imgId] {
				out.Write(sf.FormatStatus("", "Pushing tag for rev [%s] on {%s}", utils.TruncateID(imgId), ep+"repositories/"+remoteName+"/tags/"+tag))

				if err := r.PushRegistryTag(remoteName, imgId, tag, ep, repoData.Tokens); err != nil {
					return err
				}
			}
		}
	}

	if _, err := r.PushImageJSONIndex(remoteName, imageIndex, true, repoData.Endpoints); err != nil {
		return err
	}

	return nil
}

func (s *TagStore) pushImage(r *registry.Session, out io.Writer, remote, imgID, ep string, token []string, sf *utils.StreamFormatter) (checksum string, err error) {
	out = utils.NewWriteFlusher(out)
	jsonRaw, err := ioutil.ReadFile(path.Join(s.graph.Root, imgID, "json"))
	if err != nil {
		return "", fmt.Errorf("Cannot retrieve the path for {%s}: %s", imgID, err)
	}
	out.Write(sf.FormatProgress(utils.TruncateID(imgID), "Pushing", nil))

	imgData := &registry.ImgData{
		ID: imgID,
	}

	// Send the json
	if err := r.PushImageJSONRegistry(imgData, jsonRaw, ep, token); err != nil {
		if err == registry.ErrAlreadyExists {
			out.Write(sf.FormatProgress(utils.TruncateID(imgData.ID), "Image already pushed, skipping", nil))
			return "", nil
		}
		return "", err
	}

	layerData, err := s.graph.TempLayerArchive(imgID, archive.Uncompressed, sf, out)
	if err != nil {
		return "", fmt.Errorf("Failed to generate layer archive: %s", err)
	}
	defer os.RemoveAll(layerData.Name())

	// Send the layer
	log.Debugf("rendered layer for %s of [%d] size", imgData.ID, layerData.Size)

	checksum, checksumPayload, err := r.PushImageLayerRegistry(imgData.ID, utils.ProgressReader(layerData, int(layerData.Size), out, sf, false, utils.TruncateID(imgData.ID), "Pushing"), ep, token, jsonRaw)
	if err != nil {
		return "", err
	}
	imgData.Checksum = checksum
	imgData.ChecksumPayload = checksumPayload
	// Send the checksum
	if err := r.PushImageChecksumRegistry(imgData, ep, token); err != nil {
		return "", err
	}

	out.Write(sf.FormatProgress(utils.TruncateID(imgData.ID), "Image successfully pushed", nil))
	return imgData.Checksum, nil
}

// FIXME: Allow to interrupt current push when new push of same image is done.
func (s *TagStore) CmdPush(job *engine.Job) engine.Status {
	if n := len(job.Args); n != 1 {
		return job.Errorf("Usage: %s IMAGE", job.Name)
	}
	var (
		localName   = job.Args[0]
		sf          = utils.NewStreamFormatter(job.GetenvBool("json"))
		authConfig  = &registry.AuthConfig{}
		metaHeaders map[string][]string
	)

	tag := job.Getenv("tag")
	manifestBytes := job.Getenv("manifest")
	job.GetenvJson("authConfig", authConfig)
	job.GetenvJson("metaHeaders", &metaHeaders)
	if _, err := s.poolAdd("push", localName); err != nil {
		return job.Error(err)
	}
	defer s.poolRemove("push", localName)

	// Resolve the Repository name from fqn to endpoint + name
	hostname, remoteName, err := registry.ResolveRepositoryName(localName)
	if err != nil {
		return job.Error(err)
	}

	endpoint, err := registry.NewEndpoint(hostname, s.insecureRegistries)
	if err != nil {
		return job.Error(err)
	}

	img, err := s.graph.Get(localName)
	r, err2 := registry.NewSession(authConfig, registry.HTTPRequestFactory(metaHeaders), endpoint, false)
	if err2 != nil {
		return job.Error(err2)
	}

	var isOfficial bool
	if endpoint.String() == registry.IndexServerAddress() {
		isOfficial = isOfficialName(remoteName)
		if isOfficial && strings.IndexRune(remoteName, '/') == -1 {
			remoteName = "library/" + remoteName
		}
	}

	if len(tag) == 0 {
		tag = DEFAULTTAG
	}

	if endpoint.String() == registry.IndexServerAddress() || endpoint.Version == registry.APIVersion2 {
		if isOfficial {
			j := job.Eng.Job("trust_update_base")
			if err = j.Run(); err != nil {
				return job.Errorf("error updating trust base graph: %s", err)
			}
		}

		auth, err := r.GetV2Authorization(remoteName, false)
		if err != nil {
			return job.Errorf("error getting authorization: %s", err)
		}

		if len(manifestBytes) == 0 {
			// TODO Create manifest and sign
		}

		// try via manifest
		manifest, verified, err := s.verifyManifest(job.Eng, []byte(manifestBytes))
		if err != nil {
			return job.Errorf("error verifying manifest: %s", err)
		}

		if len(manifest.FSLayers) != len(manifest.History) {
			return job.Errorf("length of history not equal to number of layers")
		}

		if !verified {
			log.Debugf("Pushing unverified image")
		}

		for i := len(manifest.FSLayers) - 1; i >= 0; i-- {
			var (
				sumStr  = manifest.FSLayers[i].BlobSum
				imgJSON = []byte(manifest.History[i].V1Compatibility)
			)

			sumParts := strings.SplitN(sumStr, ":", 2)
			if len(sumParts) < 2 {
				return job.Errorf("Invalid checksum: %s", sumStr)
			}
			manifestSum := sumParts[1]

			// for each layer, check if it exists ...
			// XXX wait this requires having the TarSum of the layer.tar first
			// skip this step for now. Just push the layer every time for this naive implementation
			//shouldPush, err := r.PostV2ImageMountBlob(imageName, sumType, sum string, token []string)

			img, err := image.NewImgJSON(imgJSON)
			if err != nil {
				return job.Errorf("Failed to parse json: %s", err)
			}

			img, err = s.graph.Get(img.ID)
			if err != nil {
				return job.Error(err)
			}

			arch, err := img.TarLayer()
			if err != nil {
				return job.Errorf("Could not get tar layer: %s", err)
			}

			// Call mount blob
			exists, err := r.PostV2ImageMountBlob(remoteName, sumParts[0], manifestSum, auth)
			if err != nil {
				job.Stdout.Write(sf.FormatProgress(utils.TruncateID(img.ID), "Image push failed", nil))
				return job.Error(err)
			}
			if !exists {
				err = r.PutV2ImageBlob(remoteName, sumParts[0], manifestSum, utils.ProgressReader(arch, int(img.Size), job.Stdout, sf, false, utils.TruncateID(img.ID), "Pushing"), auth)
				if err != nil {
					job.Stdout.Write(sf.FormatProgress(utils.TruncateID(img.ID), "Image push failed", nil))
					return job.Error(err)
				}
				job.Stdout.Write(sf.FormatProgress(utils.TruncateID(img.ID), "Image successfully pushed", nil))
			} else {
				job.Stdout.Write(sf.FormatProgress(utils.TruncateID(img.ID), "Image already exists", nil))
			}
		}

		// push the manifest
		err = r.PutV2ImageManifest(remoteName, tag, bytes.NewReader([]byte(manifestBytes)), auth)
		if err != nil {
			return job.Error(err)
		}

		// done, no fallback to V1
		return engine.StatusOK
	} else {
		if err != nil {
			reposLen := 1
			if tag == "" {
				reposLen = len(s.Repositories[localName])
			}
			job.Stdout.Write(sf.FormatStatus("", "The push refers to a repository [%s] (len: %d)", localName, reposLen))
			// If it fails, try to get the repository
			if localRepo, exists := s.Repositories[localName]; exists {
				if err := s.pushRepository(r, job.Stdout, localName, remoteName, localRepo, tag, sf); err != nil {
					return job.Error(err)
				}
				return engine.StatusOK
			}
			return job.Error(err)
		}

		var token []string
		job.Stdout.Write(sf.FormatStatus("", "The push refers to an image: [%s]", localName))
		if _, err := s.pushImage(r, job.Stdout, remoteName, img.ID, endpoint.String(), token, sf); err != nil {
			return job.Error(err)
		}
		return engine.StatusOK
	}
}
